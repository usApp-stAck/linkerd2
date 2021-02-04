package watcher

import (
	"fmt"
	"strconv"
	"sync"

	sp "github.com/linkerd/linkerd2/controller/gen/apis/serviceprofile/v1alpha2"
	"github.com/linkerd/linkerd2/controller/k8s"
	labels "github.com/linkerd/linkerd2/pkg/k8s"
	"github.com/linkerd/linkerd2/pkg/util"
	logging "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
)

type ServiceWatcher struct {
	listeners map[ServiceID][]ProfileUpdateListener
	k8sAPI    *k8s.API
	log       *logging.Entry
	sync.RWMutex
}

func NewServiceWatcher(k8sAPI *k8s.API, log *logging.Entry) *ServiceWatcher {
	sw := &ServiceWatcher{
		listeners: make(map[ServiceID][]ProfileUpdateListener),
		k8sAPI:    k8sAPI,
		log:       log.WithField("component", "service-watcher"),
	}
	k8sAPI.Svc().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sw.addService,
		DeleteFunc: sw.deleteService,
		UpdateFunc: func(_, obj interface{}) { sw.addService(obj) },
	})
	return sw
}

// Subscribe TODO sync?
func (sw *ServiceWatcher) Subscribe(id ServiceID, listener ProfileUpdateListener) error {
	sw.Lock()
	defer sw.Unlock()
	svc, _ := sw.k8sAPI.Svc().Lister().Services(id.Namespace).Get(id.Name)
	if svc != nil && svc.Spec.Type == corev1.ServiceTypeExternalName {
		return invalidService(id.String())
	}
	sw.log.Infof("Establishing watch on service %s", id)
	listeners, ok := sw.listeners[id]
	if ok {
		listeners = append(listeners, listener)
		sw.listeners[id] = listeners
	} else {
		sw.listeners[id] = []ProfileUpdateListener{listener}
	}
	return nil
}

// Unsubscribe TODO sync?
func (sw *ServiceWatcher) Unsubscribe(id ServiceID, listener ProfileUpdateListener) {
	sw.Lock()
	defer sw.Unlock()
	sw.log.Infof("Stopping watch on service [%s]", id)
	listeners, ok := sw.listeners[id]
	if !ok {
		sw.log.Errorf("Cannot unsubscribe from unknown service %s", id)
		return
	}
	for i, l := range listeners {
		if l == listener {
			n := len(listeners)
			listeners[i] = listeners[n-1]
			listeners[n-1] = nil
			listeners = listeners[:n-1]
		}
	}
	sw.listeners[id] = listeners
}

func (sw *ServiceWatcher) addService(obj interface{}) {
	sw.Lock()
	defer sw.Unlock()
	service := obj.(*corev1.Service)
	if service.Namespace == kubeSystem {
		return
	}
	id := ServiceID{
		Namespace: service.Namespace,
		Name:      service.Name,
	}
	svc, err := sw.k8sAPI.Svc().Lister().Services(id.Namespace).Get(id.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		sw.log.Errorf("service not found: %s", err)
	}
	if err != nil {
		sw.log.Errorf("error getting service: %s", err)
	}
	opaquePorts, err := getServiceOpaquePortsAnnotations(svc)
	if err != nil {
		sw.log.Errorf("failed to get %s service's opaque ports annotation: %s", id, err)
	}
	if len(opaquePorts) != 0 {
		profile := sp.ServiceProfile{}
		profile.Spec.OpaquePorts = opaquePorts
		listeners, ok := sw.listeners[id]
		if ok {
			fmt.Printf("listeners={%v}, op={%v}\n", len(listeners), profile.Spec.OpaquePorts)
			for _, listener := range listeners {
				listener.Update(&profile)
			}
		}
	}
}

func (sw *ServiceWatcher) deleteService(obj interface{}) {
	sw.Lock()
	defer sw.Unlock()
	service, ok := obj.(*corev1.Service)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			sw.log.Errorf("couldn't get object from DeletedFinalStateUnknown %#v", obj)
			return
		}
		service, ok = tombstone.Obj.(*corev1.Service)
		if !ok {
			sw.log.Errorf("DeletedFinalStateUnknown contained object that is not a Service %#v", obj)
			return
		}
	}
	if service.Namespace == kubeSystem {
		return
	}
	id := ServiceID{
		Namespace: service.Namespace,
		Name:      service.Name,
	}
	delete(sw.listeners, id)
}

func getServiceOpaquePortsAnnotations(service *corev1.Service) (map[uint32]struct{}, error) {
	opaquePorts := make(map[uint32]struct{})
	annotation := service.Annotations[labels.ProxyOpaquePortsAnnotation]
	if annotation != "" {
		for _, portStr := range util.ParseOpaquePorts(annotation) {
			port, err := strconv.ParseUint(portStr, 10, 32)
			if err != nil {
				return nil, err
			}
			opaquePorts[uint32(port)] = struct{}{}
		}
	}
	return opaquePorts, nil
}
