package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/TwiN/kevent"
	str2duration "github.com/xhit/go-str2duration/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var (
	AnnotationTTL                     string
	MaximumFailedExecutionBeforePanic int
	ExecutionTimeout                  time.Duration
	ExecutionInterval                 time.Duration
	ThrottleDuration                  time.Duration
	ListLimit                         int
	ErrTimedOut                       = errors.New("execution timed out")
	listTimeoutSeconds                int64
	executionFailedCounter            = 0
	debug                             bool
)

func init() {
	AnnotationTTL = getEnv("ANNOTATION_TTL", "k8s-ttl-controller.twin.sh/ttl")
	MaximumFailedExecutionBeforePanic = getEnvInt("MAX_FAILED_EXECUTIONS", 10)
	ExecutionTimeout = getEnvDuration("EXECUTION_TIMEOUT", 20*time.Minute)
	ExecutionInterval = getEnvDuration("EXECUTION_INTERVAL", 5*time.Minute)
	ThrottleDuration = getEnvDuration("THROTTLE_DURATION", 50*time.Millisecond)
	ListLimit = getEnvInt("LIST_LIMIT", 500)
	listTimeoutSeconds = int64(getEnvInt("LIST_TIMEOUT_SECONDS", 60))
	debug = getEnv("DEBUG", "false") == "true"
}

func main() {
	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// Handle OS signals
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Initialize Kubernetes clients and event manager
	kubernetesClient, dynamicClient, err := CreateClients()
	if err != nil {
		panic("failed to create Kubernetes clients: " + err.Error())
	}
	eventManager := kevent.NewEventManager(kubernetesClient, "k8s-ttl-controller")

	ticker := time.NewTicker(ExecutionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down")
			return
		default:
			start := time.Now()
			if err := Reconcile(ctx, kubernetesClient, dynamicClient, eventManager); err != nil {
				log.Printf("Error during execution: %s", err.Error())
				executionFailedCounter++
				if executionFailedCounter > MaximumFailedExecutionBeforePanic {
					panic(fmt.Errorf("execution failed %d times: %w", executionFailedCounter, err))
				}
			} else if executionFailedCounter > 0 {
				log.Printf("Execution was successful after %d failed attempts, resetting counter to 0", executionFailedCounter)
				executionFailedCounter = 0
			}
			log.Printf("Execution took %dms, sleeping for %s", time.Since(start).Milliseconds(), ExecutionInterval)
			select {
			case <-ctx.Done():
				log.Println("Shutting down")
				return
			case <-ticker.C:
				// Continue to next iteration
			}
		}
	}
}

// Reconcile loops over all resources and deletes all sub-resources that have expired.
// Returns an error if an execution lasts longer than ExecutionTimeout.
func Reconcile(ctx context.Context, kubernetesClient kubernetes.Interface, dynamicClient dynamic.Interface, eventManager *kevent.EventManager) error {
	// Use Kubernetes' discovery API to retrieve all resources
	_, resources, err := kubernetesClient.Discovery().ServerGroupsAndResources()
	if err != nil {
		return err
	}
	if debug {
		log.Println("[Reconcile] Found", len(resources), "API resources")
	}

	// Create a context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, ExecutionTimeout)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- DoReconcile(timeoutCtx, dynamicClient, eventManager, resources)
	}()

	select {
	case <-timeoutCtx.Done():
		if timeoutCtx.Err() == context.DeadlineExceeded {
			return ErrTimedOut
		}
		return timeoutCtx.Err()
	case err := <-errChan:
		return err
	}
}

// DoReconcile goes over all API resources specified, retrieves all sub-resources, and deletes those that have expired
func DoReconcile(ctx context.Context, dynamicClient dynamic.Interface, eventManager *kevent.EventManager, resources []*metav1.APIResourceList) error {
	for _, resource := range resources {
		if len(resource.APIResources) == 0 {
			continue
		}
		gv := strings.Split(resource.GroupVersion, "/")
		gvr := schema.GroupVersionResource{}
		if len(gv) == 2 {
			gvr.Group = gv[0]
			gvr.Version = gv[1]
		} else if len(gv) == 1 {
			gvr.Version = gv[0]
		} else {
			continue
		}
		for _, apiResource := range resource.APIResources {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			// Ensure that we can list and delete the resource
			verbs := apiResource.Verbs.String()
			if !strings.Contains(verbs, "list") || !strings.Contains(verbs, "delete") {
				continue
			}
			// List all items under the resource
			gvr.Resource = apiResource.Name
			var continueToken string
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				list, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{
					TimeoutSeconds: &listTimeoutSeconds,
					Continue:       continueToken,
					Limit:          int64(ListLimit),
				})
				if err != nil {
					log.Printf("Error checking %s from %s: %s", gvr.Resource, gvr.GroupVersion(), err)
					break
				}
				if list == nil || len(list.Items) == 0 {
					break
				}
				continueToken = list.GetContinue()
				if debug {
					log.Println("Checking", len(list.Items), gvr.Resource, "from", gvr.GroupVersion())
				}
				for _, item := range list.Items {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					ttl, exists := item.GetAnnotations()[AnnotationTTL]
					if !exists {
						continue
					}
					ttlInDuration, err := str2duration.ParseDuration(ttl)
					if err != nil {
						log.Printf("[%s/%s] has an invalid TTL '%s': %s\n", apiResource.Name, item.GetName(), ttl, err)
						continue
					}
					ttlExpired := time.Now().After(item.GetCreationTimestamp().Add(ttlInDuration))
					if ttlExpired {
						durationSinceExpired := time.Since(item.GetCreationTimestamp().Add(ttlInDuration)).Round(time.Second)
						log.Printf("[%s/%s] is configured with a TTL of %s, which means it has expired %s ago", apiResource.Name, item.GetName(), ttl, durationSinceExpired)
						err := dynamicClient.Resource(gvr).Namespace(item.GetNamespace()).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
						if err != nil {
							log.Printf("[%s/%s] failed to delete: %s\n", apiResource.Name, item.GetName(), err)
							eventManager.Create(item.GetNamespace(), item.GetKind(), item.GetName(), "FailedToDeleteExpiredTTL", "Unable to delete expired resource:"+err.Error(), true)
							// Optional: Retry with GracePeriodSeconds set to &0 to force immediate deletion
						} else {
							log.Printf("[%s/%s] deleted", apiResource.Name, item.GetName())
							eventManager.Create(item.GetNamespace(), item.GetKind(), item.GetName(), "DeletedExpiredTTL", "Deleted resource because "+ttl+" or more has elapsed", false)
						}
						// Cool off a tiny bit to avoid hitting the API too often
						time.Sleep(ThrottleDuration)
					} else if debug {
						log.Printf("[%s/%s] is configured with a TTL of %s, which means it will expire in %s", apiResource.Name, item.GetName(), ttl, time.Until(item.GetCreationTimestamp().Add(ttlInDuration)).Round(time.Second))
					}
				}
				if continueToken == "" {
					break
				}
				// Cool off a tiny bit to avoid hitting the API too often
				time.Sleep(ThrottleDuration)
			}
			// Cool off a tiny bit to avoid hitting the API too often
			time.Sleep(ThrottleDuration)
		}
	}
	return nil
}

// Helper functions to get environment variables with default values
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			log.Printf("Invalid integer for %s: %v. Using default %d", key, err, defaultValue)
			return defaultValue
		}
		return value
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if valueStr, exists := os.LookupEnv(key); exists {
		value, err := time.ParseDuration(valueStr)
		if err != nil {
			log.Printf("Invalid duration for %s: %v. Using default %s", key, err, defaultValue)
			return defaultValue
		}
		return value
	}
	return defaultValue
}
