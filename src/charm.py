#!/usr/bin/env python3
# Copyright 2021 pjds
# See LICENSE file for licensing details.

"""Charm the service."""

import logging

from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
    ModelError,
)

logger = logging.getLogger(__name__)


class CharmK8SSparkCharm(CharmBase):
    """Charm the service."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        # self.framework.observe(self.on.config_changed, self._on_config_changed)
        for event in (
            # Charm events
            # (self.on.config_changed, self.on_config_changed),
            (self.on.start, self.on_start),
            # (self.on.upgrade_charm, self.on_upgrade_charm)
        ):
            self.framework.observe(*event)
        # self.framework.observe(self.on.fortune_action, self._on_fortune_action)
        self._stored.set_default(things=[])

    def _apply_spec(self, spec):
        # Only apply the spec if this unit is a leader.
        if self.framework.model.unit.is_leader():
            self.framework.model.pod.set_spec(spec)
            self._stored.spec = spec


    def make_pod_spec(self):
        config = self.framework.model.config

        return {
            'version': 3,
            'containers': [
                {
                    'envConfig': {
                        'SPARK_MODE': 'master',
                        'SPARK_DAEMON_MEMORY': '',
                        'SPARK_MASTER_PORT': '7077',
                        'SPARK_MASTER_WEBUI_PORT': '8080'
                    },
                    'image': config["image"],
                    'imagePullPolicy': 'IfNotPresent',
                    'kubernetes': {
                        'livenessProbe': {
                            'failureThreshold': 6,
                            'httpGet': {
                                'path': '/',
                                'port': 8080,
                                'scheme': 'HTTP'
                        },
                        'initialDelaySeconds': 180,
                        'periodSeconds': 20,
                        'successThreshold': 1,
                        'timeoutSeconds': 5
                        },
                        'readinessProbe': {
                            'failureThreshold': 6,
                            'httpGet': {
                                'path': '/',
                                'port': 8080,
                                'scheme': 'HTTP'
                            },
                            'initialDelaySeconds': 30,
                            'periodSeconds': 10,
                            'successThreshold': 1,
                            'timeoutSeconds': 5
                            },
                        # 'resources': {
                        #     'requests': {
                        #         'cpu': '100m'
                        #     }
                        # },
                    },
                    'name': 'spark-master',
                    'ports': [
                            {
                                'containerPort': 8080,
                                'name': 'http',
                                'protocol': 'TCP'
                            },
                            {
                                'containerPort': 7077,
                                'name': 'cluster',
                                'protocol': 'TCP'
                            }
                        ]
                }
            ],
            'kubernetesResources': {
                    #  'serviceAccounts':[{
                    #  'name': 'default',
                    #  'automountServiceToken': True,
                    #  # Check cluster
                    #  #  'subdomain': 'juju-app-spark-headless',
                    #  #  'terminationGracePeriodSeconds': 30,
                    #  }],
                    #  'tolerations': [
                    #  {
                        #  'effect': 'NoExecute',
                        #  'key': 'node.kubernetes.io/not-ready',
                        #  'operator': 'Exists',
                        #  'tolerationSeconds': 300
                    #  },
                    #  {
                        #  'effect': 'NoExecute',
                        #  'key': 'node.kubernetes.io/unreachable',
                        #  'operator': 'Exists',
                        #  'tolerationSeconds': 300
                    #  }
                    #  ],
                #  }],
                    'pod': {
                        'annotations': {
                            'kubernetes.io/limit-ranger': 'limitranger plugin set: cpu request for container spark-master'
                            #  'helm.sh/chart': 'spark-5.0.1',
                        },
                        'labels': {
                            'foo': 'bax',
                            'app.kubernetes.io/name': 'spark',
                            #  'controller-revision-hash': 'my-release-spark-master-cc85fcbf4',
                            #  'statefulset.kubernetes.io/pod-name': 'my-release-spark-master-0'
                        },
                        # 'activeDeadlineSeconds': 10,
                        # 'terminationMessagePath': '/dev/termination-log',
                        # 'terminationMessagePolicy': 'File',
                        # 'restartPolicy': 'OnFailure',
                        # 'terminationGracePeriodSeconds': 30,
                        # 'automountServiceAccountToken': True,
                        #  'hostNetwork': True,
                        #  'hostPID': True,
                        'dnsPolicy': 'ClusterFirstWithHostNet',
                        'securityContext': {
                            'runAsNonRoot': True,
                            'fsGroup': 14
                        },
                        'priorityClassName': 'top',
                        'priority': 30,
                        'readinessGates': [
                            {
                                'conditionType': 'PodScheduled',
                            },
                        ],
                    }
            }
        }

    def on_start(self, event):
        """Called when the charm is being installed"""
        unit = self.model.unit

        unit.status = MaintenanceStatus("Applying pod spec")

        new_pod_spec = self.make_pod_spec()
        self._apply_spec(new_pod_spec)

        unit.status = ActiveStatus()

    def on_config_changed(self, _):
        unit = self.model.unit
        # current = self.config["thing"]

        new_spec = self.make_pod_spec()

        # if self._stored.spec != new_spec:
        unit.status = MaintenanceStatus("Appling new pod spec")

        self._apply_spec(new_spec)

        unit.status = ActiveStatus()

    def _on_fortune_action(self, event):
        fail = event.params["fail"]
        if fail:
            event.fail(fail)
        else:
            event.set_results({"fortune": "A bug in the code is worth two in the documentation."})

    def on_upgrade_charm(self, event):
        """Upgrade the charm."""
        # raise NotImplementedError("TODO")
        unit = self.model.unit

        # Mark the unit as under Maintenance.
        unit.status = MaintenanceStatus("Upgrading charm")

        self.on_start(event)

        # When maintenance is done, return to an Active state
        unit.status = ActiveStatus()


if __name__ == "__main__":
    main(CharmK8SSparkCharm)
