package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"adapter/internal/api"
	"adapter/internal/db"
	"adapter/internal/exec"
	"adapter/internal/k8s"
	"adapter/internal/sandbox"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "HTTP listen address")
		namespace = flag.String("namespace", "default", "k8s namespace for sandbox pods")
		dbURL     = flag.String("db-url", defaultDBURL(), "postgres connection url (also reads DATABASE_URL env)")
	)
	flag.Parse()

	ctx := context.Background()

	// Bring up the database connection and apply schema before anything else
	// so a misconfigured DB fails fast at startup rather than on first write.
	database, err := db.New(ctx, *dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer database.Close()

	if err := database.Migrate(ctx); err != nil {
		log.Fatalf("db migrate: %v", err)
	}
	log.Printf("db connected, schema applied")

	// k8s.New picks up in-cluster config, then $KUBECONFIG, then the k3s
	// default at /etc/rancher/k3s/k3s.yaml. Set $KUBECONFIG to override on
	// the dev box.
	k8sClient, err := k8s.New(*namespace)
	if err != nil {
		log.Fatalf("k8s client: %v", err)
	}

	// Chunk B: sandbox.Manager is now postgres-backed. The agent-connection
	// cache stays in memory (gRPC connections can't survive restart anyway)
	// but the source of truth for sandbox state is the DB. Reconcile reads
	// every running row and either redials its agent or marks it host_failed
	// if the pod is gone, before HTTP serving starts.
	mgr := sandbox.NewManager(database, k8sClient)
	if err := mgr.Reconcile(ctx); err != nil {
		log.Fatalf("reconcile sandboxes: %v", err)
	}

	// Chunk C: exec.Registry now persists every exec to the DB. Reconcile
	// flips any rows left at status=running (which can only be stale, since
	// agent execs die with the adapter's gRPC stream) to errored with
	// completion_err=adapter_restarted.
	registry := exec.NewRegistry(database)
	if err := registry.Reconcile(ctx); err != nil {
		log.Fatalf("reconcile execs: %v", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.NewRouter(mgr, registry),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("sandbox-oss adapter listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Print("shutdown signal received, draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// defaultDBURL returns $DATABASE_URL if set, otherwise the dev-box default
// pointing at a local postgres with the sandbox_oss user/database.
func defaultDBURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://sandbox_oss:sandbox_oss@localhost:5432/sandbox_oss?sslmode=disable"
}