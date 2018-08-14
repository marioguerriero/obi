import os

import yaml

import base64
from random import randint

import kubernetes as k8s

import grpc

import master_rpc_service_pb2
import master_rpc_service_pb2_grpc

from logger import log

import utils

from generic_client import GenericClient


class FieldMissingError(Exception):
    """
    This exception should be triggered when a mandatory field has been
    omitted from a configuration file
    """
    pass


class KubernetesClient(GenericClient):
    #############
    #  START: Abstract methods from GenericClient
    #############
    def __init__(self, user_config):
        """
        Create k8s client object and other basic objects
        """
        # Create lookup maps for functions
        self._create_f = {
            'job': self._create_job,
            'infrastructure': self._create_infrastructure,
        }
        self._get_f = {
            'job': self._get_jobs,
            'infrastructure': self._get_infrastructures,
        }
        self._describe_f = {
            'job': self._describe_job,
            'infrastructure': self._describe_infrastructure(),
        }
        self._delete_f = {
            'job': self._delete_job,
            'infrastructure': self._delete_infrastructure,
        }

        # Load user configuration
        self._user_config = user_config

        # Load cluster configuration
        k8s.config.load_kube_config()

        # Prepare client objects
        self._core_client = k8s.client.CoreV1Api()
        self._apps_client = k8s.client.AppsV1Api()

    def get_objects(self, **kwargs):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """
        log.info('Get request: {}'.format(kwargs))
        self._get_f[kwargs['type']](**kwargs)

    def create_object(self, **kwargs):
        """
        Generates a new platform service for the given configuration
        :return:
        """
        log.info('Create request: {}'.format(kwargs))
        self._create_f[kwargs['type']](**kwargs)

    def delete_object(self, **kwargs):
        """
        Deletes all k8s objects for the given platform
        :return:
        """
        log.info('Delete request: {}'.format(kwargs))
        self._delete_f[kwargs['type']](**kwargs)

    def describe_object(self, **kwargs):
        """
        Submit a job to OBI according to the given request
        :return:
        """
        log.info('Describe request: {}'.format(kwargs))
        self._describe_f[kwargs['type']](**kwargs)

    #############
    #  END: Abstract methods from GenericClient
    #############

    def _create_job(self, **kwargs):
        """
        Submit a job to OBI according to the given request
        :param submit_job_request:
        :return:
        """
        # Obtain connection information
        host, port = self._get_connection_info(
            kwargs['job_infrastructure'])

        # Check if the job type is valid or not
        sup_types = [t['name'] for t in self._user_config['supportedJobTypes']]
        if kwargs['job_type'] not in sup_types:
            log.error('{} job type is invalid. '
                      'Supported job types: {}'.format(kwargs['job_type'],
                                                       sup_types))

        # Build submit job request object
        job = master_rpc_service_pb2.Job()
        job.executablePath = kwargs['job_path']
        job.type = utils.map_job_type(kwargs['job_type'])

        infrastructure = master_rpc_service_pb2.Infrastructure()

        req = master_rpc_service_pb2.SubmitJobRequest(
            job=job,
            infrastructure=infrastructure)

        # Create connection object
        with grpc.insecure_channel('{}:{}'.format(host, port)) as channel:
            stub = master_rpc_service_pb2_grpc.ObiMasterStub(channel)
            # Submit job creation request
            stub.SubmitJob(req)

    def _create_infrastructure(self, **kwargs):
        """
        Submit an infrastructure to OBI according to the given request
        :param submit_job_request:
        :return:
        """
        # Read deployment YAML file
        with open(kwargs['infrastructure_path'], 'r') as df:
            deployment = yaml.load(df)

        # Get name
        name = deployment['name'] if 'name' in deployment \
            else self._object_name_generator()
        log.info('Creating infrastructure named "{}"'.format(name))

        # Get namespace
        namespace = deployment['namespace'] if 'namespace' in deployment \
            else self._user_config['kubernetesNamespace']
        log.info('Kubernetes namespace: {}'.format(name))

        # Check if the mandatory fields were specified
        fields = ['serviceAccountPath', 'projectId', 'region', 'zone']
        for f in fields:
            if f not in deployment:
                raise FieldMissingError('"{}" field is mandatory'.format(f))

        # Generate secret
        secret_name = self._create_secret_from_file(
            namespace, deployment['serviceAccountPath'])

        # Generate label to be used to match services to master deployment
        master_selector = self._object_name_generator(
            prefix='{}-deployment'.format(
                self._user_config['kubernetesNamespace']))
        log.info('Master selector label: {}'.format(master_selector))

        # Generate label selector for predictive component
        predictor_selector = self._object_name_generator(
            prefix='{}-predictor-deployment'.format(
                self._user_config['kubernetesNamespace']))
        log.info('Predictor selector label: {}'.format(predictor_selector))

        # Create master service
        self._create_master_service(
            namespace, master_selector)
        log.info('Master service created')

        # Create heartbeat service
        heartbeat_host, heartbeat_port = self._create_heartbeat_service(
            namespace, master_selector)
        log.info('Heartbeat service created')

        # Create predictive service
        predictor_name = self._object_name_generator(
            prefix='-'.join([name, 'predictor']))
        pred_host, pred_port = \
            self._create_predictive_component(predictor_name, namespace,
                                              predictor_selector)
        log.info('Predictor component created')

        # Create config map to be used in the deployment
        config_map_name = self._create_config_map(
            namespace,
            heartbeatHost=heartbeat_host, heartbeatPort=heartbeat_port,
            projectId=deployment['projectId'],
            region=deployment['region'], zone=deployment['zone'],
            masterPort=self._user_config['defaultMasterPort'],
            predictorHost=pred_host, predictorPort=pred_port)

        # Create deployment
        self._create_infrastructure_deployment(
            name, namespace, deployment['projectId'], secret_name,
            master_selector,
            config_map_name)
        log.info('Infrastructure successfully created')

    def _delete_job(self, **kwargs):
        """
        Deletes all k8s objects for the given job
        :return:
        """

    def _delete_infrastructure(self, **kwargs):
        """
        Deletes all k8s objects for the given infrastructure
        :return:
        """

    def _get_jobs(self, **kwargs):
        """
        Return information for all the running jobs
        :return:
        """

    def _get_infrastructures(self, **kwargs):
        """
        Return information for all the infrastructures
        :return:
        """

    def _describe_job(self, **kwargs):
        """
        Return detailed information for the given job
        :return:
        """

    def _describe_infrastructure(self, **kwargs):
        """
        Return detailed information for the given infrastructure
        :return:
        """

    #############
    #  START: Generic utility functions
    #############
    def _get_connection_info(self, infrastructure):
        """
        Given an infrastructure object, this function returns a tuple
        with IP address and port for connecting to the given infrastructure
        :param infrastructure:
        :return:
        """
        # TODO
        return self._user_config['masterHost'], self._user_config['masterPort']

    def _discover_services(self):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """

    def _object_name_generator(self, prefix=None):
        """
        This method simply generates a random name for an OBI infrastructure.
        It is called when the user does not specify a name while creating an
        infrastructure.
        :return:
        """
        if prefix is None:
            prefix = self._user_config['masterHost']
        return '{}-{}'.format(prefix, randint(1000, 9999))

    def _create_secret_from_file(self, namespace, path):
        """
        This function create a namespaced secret from the given file and return
        the secret name on k8s
        :param namespace:
        :param path:
        :return:
        """
        # Open file
        with open(path, 'r') as f:
            secret_content = f.read()

        # It was noticed that any newline character or '\n' sequence
        # causes the secret not to be decoded properly. That's why
        # I am avoiding those cases here
        secret_content = secret_content.replace('\n', '').replace('\r', '') \
            .replace('\\n', '').replace('\\r', '')

        # Generate secret
        secret_name = self._object_name_generator(
            prefix='{}-secret'.format(
                self._user_config['kubernetesNamespace']))
        secret = k8s.client.V1Secret()
        secret.metadata = k8s.client.V1ObjectMeta()
        secret.metadata.name = secret_name
        encoded_secret = base64.b64encode(secret_content.encode('utf-8'))
        encoded_string = encoded_secret.decode('ascii')
        secret.data = {
            secret_name: encoded_string
        }

        # Send secret creation request
        try:
            self._core_client.create_namespaced_secret(namespace,
                                                                      secret,
                                                                      pretty='true')
            return secret_name
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_secret: "
                "%s" % e)

    def _create_infrastructure_deployment(self, name, namespace, project_id,
                                          sa_secret, label, config_map_name):
        """
        This function is used to generate the deployment for the OBI
        master for a certain infrastructure. This function returns the
        name of the selectors to be used by services to address
        the generated deployment
        :return:
        """
        # Create Deployment Object
        deployment = k8s.client.V1Deployment()

        # Create metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.namespace = namespace
        deployment.metadata = metadata

        # Create Spec object
        deployment.spec = self._build_deployment_spec_object(label,
                                                             project_id,
                                                             sa_secret,
                                                             config_map_name)

        # Send deployment creation request
        try:
            self._apps_client.create_namespaced_deployment(
                namespace, deployment, pretty='true')
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->create_namespaced_deployment: "
                "%s" % e)

    def _build_deployment_spec_object(self, label, project_id,
                                      sa_secret, config_map_name):
        """
        This function simply creates a spec object to attach to the
        deployment of a new OBI master
        :param label:
        :param project_id:
        :param sa_secret:
        :return: the generated spec object
        """
        # Build selector object
        selector = k8s.client.V1LabelSelector()
        selector.match_labels = {
            self._user_config['defaultServiceSelectorName']: label
        }

        # Build spec template object
        template = k8s.client.V1PodTemplateSpec()
        meta = k8s.client.V1ObjectMeta()
        meta.labels = {
            self._user_config['defaultServiceSelectorName']: label
        }
        template.metadata = meta

        # Build containers list
        container = k8s.client.V1Container(name=self._object_name_generator(
            prefix="{}-master-container".format(
                self._user_config['kubernetesNamespace'])
        ))
        container.image = self._user_config['masterImage']

        # Volume mount for secret
        volume_secret_mount_name = self._object_name_generator(
            prefix='{}-volumne-mount'.format(
                self._user_config['kubernetesNamespace']))
        volume_mount_secret = k8s.client.V1VolumeMount(
            name=volume_secret_mount_name,
            mount_path=self._user_config['secretMountPath'])

        # Volume mount for config map
        volume_mount_config_map_name = self._object_name_generator(
            prefix='{}-volumne-mount'.format(
                self._user_config['kubernetesNamespace']))
        volume_mount_config_map = k8s.client.V1VolumeMount(
            name=volume_mount_config_map_name,
            mount_path=self._user_config['configMountPath'])

        container.volume_mounts = [
            volume_mount_secret, volume_mount_config_map
        ]

        # Environment variables
        env_proj = k8s.client.V1EnvVar(
            name='GOOGLE_CLOUD_PROJECT', value=project_id)

        env_creds = k8s.client.V1EnvVar(
            name='GOOGLE_APPLICATION_CREDENTIALS', value=os.path.join(
                self._user_config['secretMountPath'], sa_secret))

        env_config = k8s.client.V1EnvVar(
            name='CONFIG_PATH', value=os.path.join(
                self._user_config['configMountPath'], config_map_name)
        )

        container.env = [
            env_proj, env_creds, env_config
        ]

        # Build template spec object
        template_spec = k8s.client.V1PodSpec(containers=[
            container
        ])

        # Volume for SA secret
        volume_secret = k8s.client.V1Volume(name=volume_secret_mount_name)
        volume_secret.secret = k8s.client.V1SecretVolumeSource()
        volume_secret.secret.secret_name = sa_secret

        # Volume for config map
        volume_config_map = k8s.client.V1Volume(name=volume_mount_config_map_name)
        volume_config_map.config_map = k8s.client.V1ConfigMapVolumeSource()
        volume_config_map.config_map.name = config_map_name

        template_spec.volumes = [
            volume_secret, volume_config_map
        ]
        template.spec = template_spec

        # Build spec object
        spec = k8s.client.V1DeploymentSpec(
            selector=selector, template=template)
        spec.replicas = self._user_config['masterReplicas']

        return spec

    def _create_master_service(self, namespace, label):
        """
        This function a OBI master service and returns its
        public IP address and port
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = self._object_name_generator(
            prefix='{}-master-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        metadata.namespace = namespace
        service.metadata = metadata

        # Spec object
        spec = k8s.client.V1ServiceSpec()
        spec.type = 'LoadBalancer'
        port = k8s.client.V1ServicePort(
            port=self._user_config['defaultMasterPort'])
        port.protocol = 'TCP'
        spec.ports = [
            port
        ]
        spec.selector = {
            self._user_config['defaultServiceSelectorName']: label
        }

        service.spec = spec

        try:
            api_response = self._core_client.create_namespaced_service(
                namespace, service, pretty='true')

            return api_response.spec.load_balancer_ip, port.port
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None

    def _create_heartbeat_service(self, namespace, label):
        """
        This function a OBI master service and returns its
        public IP address and port
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = self._object_name_generator(
            prefix='{}-heartbeat-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        metadata.namespace = namespace
        service.metadata = metadata

        # Spec object
        spec = k8s.client.V1ServiceSpec()
        spec.type = 'LoadBalancer'
        port = k8s.client.V1ServicePort(
            port=self._user_config['defaultHeartbeatPort']
        )
        port.protocol = 'UDP'
        spec.ports = [
            port
        ]
        spec.selector = {
            self._user_config['defaultServiceSelectorName']: label
        }

        service.spec = spec

        try:
            api_response = self._core_client.create_namespaced_service(
                namespace, service, pretty='true')

            return api_response.spec.load_balancer_ip, port.port
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None

    def _create_config_map(self, namespace, **kwargs):
        """
        This function is used to generate a k8s config map, to be passed
        to a deployment so that the OBI master can use it to retrieve
        its configuration details.
        :param namespace:
        :return: the name of the generated config map
        """
        # Generate a name for the config map
        name = self._object_name_generator(
            prefix='{}-config-map'.format(
                self._user_config['kubernetesNamespace']))

        # Generate config map content
        cm_content = yaml.dump(kwargs)

        # Create config map object
        config_map = k8s.client.V1ConfigMap()
        config_map.data = {
            self._user_config['defaultConfigMapName']: cm_content
        }

        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        config_map.metadata = metadata

        try:
            api_response = self._core_client.create_namespaced_config_map(
                namespace, config_map, pretty='true')

            return name
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->create_namespaced_config_map: %s\n" % e)
            return None

    def _create_predictive_component(self, name, namespace, label):
        """
        This function creates and deploys all the k8s objects required for the
        predictive component. It then returns IP address and port information
        to contact the predictive server.
        :return:
        """
        # Create service for predictive component
        pred_host, pred_port = self._create_predictive_service(
            namespace, label)

        # Create config map to be used in the deployment
        config_map_name_pred = self._create_config_map(
            namespace)  # This is empty for now

        # Create deployment for predictive component
        self._create_predictive_deployment(name, namespace, label,
                                           config_map_name_pred)

        # Return host and port to contact the predictor component
        return pred_host, pred_port

    def _create_predictive_deployment(self, name, namespace,
                                      label, config_map_name_pred):
        """
        Build and generate deployment for the OBI predictive component
        :return:
        """
        # Get deployment object
        deployment = self._build_predictor_deployment(name, namespace,
                                                      label,
                                                      config_map_name_pred)

        # Send deployment creation request
        try:
            self._apps_client.create_namespaced_deployment(
                namespace, deployment, pretty='true')
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->create_namespaced_deployment: "
                "%s" % e)

    def _build_predictor_deployment(self, name, namespace,
                                    label, config_map_name):
        """
        This function builds and returns a deployment object for the
        predictive component of OBI
        :return:
        """
        # Create Deployment Object
        deployment = k8s.client.V1Deployment()

        # Create metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.namespace = namespace
        deployment.metadata = metadata

        # Build selector object
        selector = k8s.client.V1LabelSelector()
        selector.match_labels = {
            self._user_config['defaultServiceSelectorName']: label
        }

        # Build spec template object
        template = k8s.client.V1PodTemplateSpec()
        meta = k8s.client.V1ObjectMeta()
        meta.labels = {
            self._user_config['defaultServiceSelectorName']: label
        }
        template.metadata = meta

        # Build containers list
        container = k8s.client.V1Container(name=self._object_name_generator(
            prefix="{}-predictor-container".format(
                self._user_config['kubernetesNamespace'])
        ))
        container.image = self._user_config['predictorImage']

        # Volume mount for config map
        volume_mount_config_map_name = self._object_name_generator(
            prefix='{}-volumne-mount'.format(
                self._user_config['kubernetesNamespace']))
        volume_mount_config_map = k8s.client.V1VolumeMount(
            name=volume_mount_config_map_name,
            mount_path=self._user_config['configMountPath'])

        container.volume_mounts = [
            volume_mount_config_map
        ]

        template_spec = k8s.client.V1PodSpec(containers=[
            container
        ])

        # Volume for config map
        volume_config_map = k8s.client.V1Volume(
            name=volume_mount_config_map_name)
        volume_config_map.config_map = k8s.client.V1ConfigMapVolumeSource()
        volume_config_map.config_map.name = config_map_name

        template_spec.volumes = [
            volume_config_map
        ]
        template.spec = template_spec

        # Build spec object
        spec = k8s.client.V1DeploymentSpec(selector=selector, template=template)
        spec.replicas = self._user_config['predictorReplicas']

        # Attach spec to deployment
        deployment.spec = spec

        return deployment

    def _create_predictive_service(self, namespace, label):
        """
        This function creates the service for the predictive component
        and returns it IP address and port information
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = self._object_name_generator(
            prefix='{}-predictive-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        metadata.namespace = namespace
        service.metadata = metadata

        # Spec object
        spec = k8s.client.V1ServiceSpec()
        spec.type = 'ClusterIP'
        port = k8s.client.V1ServicePort(
            port=self._user_config['defaultPredictorPort']
        )
        port.protocol = 'TCP'
        spec.ports = [
            port
        ]
        spec.selector = {
            self._user_config['defaultServiceSelectorName']: label
        }

        service.spec = spec

        try:
            api_response = self._core_client.create_namespaced_service(
                namespace, service, pretty='true')
            return api_response.spec.load_balancer_ip, port.port
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None
    #############
    #  END: Generic utility functions
    #############
