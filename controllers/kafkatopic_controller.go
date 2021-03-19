/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	topic_manager "github.com/hendrikhh/kafka-topic-operator/internal/topic-manager"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafkav1alpha1 "github.com/hendrikhh/kafka-topic-operator/api/v1alpha1"
)

// KafkaTopicReconciler reconciles a KafkaTopic object
type KafkaTopicReconciler struct {
	client.Client
	Log          logr.Logger
	Scheme       *runtime.Scheme
	TopicManager *topic_manager.TopicManager
}

// +kubebuilder:rbac:groups=kafka.haase.de,resources=kafkatopics,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.haase.de,resources=kafkatopics/status,verbs=get;update;patch

func (r *KafkaTopicReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("kafkatopic", req.NamespacedName)

	var topic kafkav1alpha1.KafkaTopic
	if err := r.Get(ctx, req.NamespacedName, &topic); err != nil {
		log.Error(err, "unable to fetch KafkaTopic")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.TopicManager.UpsertTopic(topic.Spec.Name, topic.Spec.Partitions, topic.Spec.Replicas, topic.Spec.Config); err != nil {
		return ctrl.Result{}, fmt.Errorf("can't upsert topic: %v", err)
	}

	return ctrl.Result{}, nil
}

func (r *KafkaTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kafkav1alpha1.KafkaTopic{}).
		Complete(r)
}
