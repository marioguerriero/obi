import os
import sys

import yaml

import base64
from random import randint

from google.cloud import storage

import kubernetes as k8s

import grpc

import master_rpc_service_pb2
import master_rpc_service_pb2_grpc

from logger import log

import utils

from generic_client import GenericClient


class InvalidInfrastructureName(Exception):
    """
    This exception should be triggered when a mandatory field has been
    omitted from a configuration file
    """
    pass


class FieldMissingError(Exception):
    """
    This exception should be triggered when a mandatory field has been
    omitted from a configuration file
    """
    pass


class MissingServiceForInfrastructure(Exception):
    """
    This exception should be triggered when a mandatory field has been
    omitted from a configuration file
    """
    pass


class ServiceUnreachableException(Exception):
    """
    This exception is triggered when the user tries to contact the OBI master
    but it is not reachable
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
            'infrastructure': self._describe_infrastructure,
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
        type = kwargs['type']
        type = type[:-1] if type[-1] == 's' else type
        self._get_f[type](**kwargs)

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
        try:
            host, port = self._get_connection_information(
                self._user_config['kubernetesNamespace'],
                name=kwargs['job_infrastructure'])
        except InvalidInfrastructureName as e:
            log.fatal(e)
            sys.exit(1)
        except ServiceUnreachableException as sue:
            log.fatal(sue)
            sys.exit(1)

        # Check if the job type is valid or not
        sup_types = [t['name'] for t in self._user_config['supportedJobTypes']]
        if kwargs['job_type'] not in sup_types:
            log.error('{} job type is invalid. '
                      'Supported job types: {}'.format(kwargs['job_type'],
                                                       sup_types))

        # Build submit job request object
        job_args = kwargs['job_args']
        if job_args is not None:
            job_args = job_args[1:]
            job_args = ' '.join(job_args)
        req = master_rpc_service_pb2.JobSubmissionRequest(
            executablePath=kwargs['job_path'],
            infrastructure=kwargs['job_infrastructure'],
            type=utils.map_job_type(kwargs['job_type']),
            priority=int(kwargs['job_priority']),
            jobArgs=job_args
        )

        # Create connection object
        with grpc.insecure_channel('{}:{}'.format(host, port)) as channel:
            stub = master_rpc_service_pb2_grpc.ObiMasterStub(channel)
            # Check if the user wants to execute a local script
            is_local = True
            try:
                f = open(req.executablePath, 'r')
                f.close()
            except IOError:
                is_local = False
            if is_local:
                # Upload the file
                client = storage.Client()
                bucket = client.get_bucket(self._user_config['tmpBucket'])
                # Then do other things...
                md5 = utils.md5(req.executablePath)
                blob_name = '{}/{}-{}'.format(
                    self._user_config['tmpBlobPath'],
                    md5,
                    os.path.basename(req.executablePath)
                )
                gcs_path = 'gs://{}/{}'.format(self._user_config['tmpBucket'],
                                               blob_name)
                log.info('Uploading local file to {}'.format(gcs_path))
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(filename=req.executablePath)
                req.executablePath = gcs_path
            # Submit job creation request
            log.info('Sending job submission request')
            stub.SubmitJob(req)

        log.info('Job submitted successfully')

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
        log.info('Kubernetes namespace: {}'.format(namespace))

        # Check if infrastructure already exist
        deployments = self._get_master_deployments(namespace)
        deployment_names = [d.metadata.name for d in deployments]
        if name in deployment_names:
            log.fatal('Infrastructure "{}" already exist'.format(name))
            sys.exit(1)

        # Check if the mandatory fields were specified
        fields = ['dataprocServiceAccountPath', 'gcsServiceAccountPath',
                  'projectId', 'region', 'zone']
        for f in fields:
            if f not in deployment:
                raise FieldMissingError('"{}" field is mandatory'.format(f))

        # Generate secret
        dataproc_secret_name = self._create_secret_from_file(
            namespace, deployment['dataprocServiceAccountPath'])

        gcs_secret_name = self._create_secret_from_file(
            namespace, deployment['gcsServiceAccountPath'])

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

        # Generate label selector for API component
        api_selector = self._object_name_generator(
            prefix='{}-api-deployment'.format(
                self._user_config['kubernetesNamespace']))
        log.info('API selector label: {}'.format(api_selector))

        # Create master service
        master_service_name = self._object_name_generator(
            prefix='{}-master-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        self._create_master_service(master_service_name,
                                    namespace, master_selector)
        log.info('Master service created')

        # Create heartbeat service
        heartbeat_service_name = self._object_name_generator(
            prefix='{}-heartbeat-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        log.info('Creating heartbeat service. '
                 'This operation may take a while')
        heartbeat_host, heartbeat_port = self._create_heartbeat_service(
            heartbeat_service_name, namespace, master_selector)
        log.info('Heartbeat service created')

        # Create predictive service
        predictor_deployment_name = self._object_name_generator(
            prefix='-'.join([name, 'predictor']))
        predictor_service_name = self._object_name_generator(
            prefix='{}-predictive-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        pred_host, pred_port = \
            self._create_predictive_component(predictor_deployment_name,
                                              predictor_service_name,
                                              namespace,
                                              deployment['projectId'],
                                              gcs_secret_name,
                                              predictor_selector)
        log.info('Predictor component created')

        # Create config map to be used in the deployment
        config_map_name = self._object_name_generator(
            prefix='{}-config-map'.format(
                self._user_config['kubernetesNamespace']))
        self._create_config_map(
            config_map_name,
            namespace,
            heartbeatHost=heartbeat_host, heartbeatPort=heartbeat_port,
            projectId=deployment['projectId'],
            region=deployment['region'], zone=deployment['zone'],
            masterPort=self._user_config['defaultMasterPort'],
            predictorHost=pred_host, predictorPort=pred_port,
            dbType=deployment['dbType'], dbHost=deployment['dbHost'],
            dbPort=deployment['dbPort'], dbUser=deployment['dbUser'],
            dbPassword=deployment['dbPassword'], dbName=deployment['dbName'],
            schedulingLevels=deployment['schedulingLevels'],
            priorityMap=deployment['priorityMap'])

        # Create API service
        api_deployment_name = self._object_name_generator(
            prefix='-'.join([name, 'api']))
        api_service_name = self._object_name_generator(
            prefix='{}-api-service'.format(
                self._user_config['kubernetesNamespace'])
        )
        self._create_api_component(api_deployment_name, api_service_name,
                                   namespace, api_selector, config_map_name)
        log.info('API service created')

        # Create deployment
        self._create_infrastructure_deployment(
            name, namespace, deployment['projectId'],
            dataproc_secret_name,
            master_selector,
            config_map_name,
            master_service_name,
            heartbeat_service_name,
            predictor_service_name,
            predictor_deployment_name,
            api_service_name,
            api_deployment_name)
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
        # Get requested infrastructure deployment
        master_deployment = self._get_deployment_object(
            self._user_config['kubernetesNamespace'],
            kwargs['infrastructure_name'])

        # Get predictor deployment
        predictor_deployment = self._get_deployment_object(
            self._user_config['kubernetesNamespace'],
            master_deployment.metadata.annotations[
                self._user_config['predictorDeploymentName']])

        # Collect names of the objects which should be deleted
        deployment_names = [
            kwargs['infrastructure_name'],
            master_deployment.metadata.annotations[
                self._user_config['predictorDeploymentName']],
            master_deployment.metadata.annotations[
                self._user_config['apiDeploymentName']]
        ]

        service_names = [
            master_deployment.metadata.annotations[
                self._user_config['masterServiceName']],
            master_deployment.metadata.annotations[
                self._user_config['heartbeatServiceName']],
            master_deployment.metadata.annotations[
                self._user_config['predictorServiceName']],
            master_deployment.metadata.annotations[
                self._user_config['apiServiceName']],
        ]

        secret_names = [
            master_deployment.metadata.annotations[
                self._user_config['serviceAccountSecretName']
            ],
            predictor_deployment.metadata.annotations[
                self._user_config['predictorServiceAccountName']
            ]
        ]

        configmaps = [
            master_deployment.metadata.annotations[
                self._user_config['masterConfigMapName']
            ],
            predictor_deployment.metadata.annotations[
                self._user_config['predictorConfigMapName']
            ]
        ]

        # Delete all the objects
        log.info('Deleting infrastructure objects')

        for d in deployment_names:
            self._delete_deployment(
                self._user_config['kubernetesNamespace'], d)

        log.info('All deployments deleted')

        for s in service_names:
            self._delete_service(
                self._user_config['kubernetesNamespace'], s)

        log.info('All services deleted')

        for s in secret_names:
            self._delete_secret(
                self._user_config['kubernetesNamespace'], s)

        log.info('All secrets deleted')

        for c in configmaps:
            self._delete_configmap(self._user_config['kubernetesNamespace'], c)

        log.info('All ConfigMaps deleted')

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
        namespace = self._user_config['kubernetesNamespace']

        # Get deployments list
        deployments = self._get_master_deployments(namespace)

        if len(deployments) == 0:
            log.info("No available infrastructures")
            return

        # Print fancy message
        log.info('Available infrastructures:\n')
        raw_format = '{:25} {:15} {:>10}'
        print(raw_format.format('INFRASTRUCTURE', 'IP', 'PORT'))
        for d in deployments:
            try:
                ip, port = self._get_connection_information(
                    namespace, deployment=d)
            except ServiceUnreachableException as e:
                log.fatal(e)
                sys.exit(1)
            print(raw_format.format(d.metadata.name, ip, port))

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
        secret_content = secret_content.replace('\n', '').replace('\r', '')

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
                                          sa_secret, label, config_map_name,
                                          master_service_name,
                                          heartbeat_service_name,
                                          predictor_service_name,
                                          predictor_deployment_name,
                                          api_service_name,
                                          api_deployment_name):
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
        metadata.annotations = {
            self._user_config['typeMetadata']: self._user_config['masterType'],
            self._user_config['masterServiceName']: master_service_name,
            self._user_config['heartbeatServiceName']: heartbeat_service_name,
            self._user_config['predictorServiceName']: predictor_service_name,
            self._user_config['predictorDeploymentName']:
                predictor_deployment_name,
            self._user_config['apiServiceName']: api_service_name,
            self._user_config['apiDeploymentName']:
                api_deployment_name,
            self._user_config['serviceAccountSecretName']: sa_secret,
            self._user_config['masterConfigMapName']: config_map_name,
        }
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
        container.image_pull_policy = 'Always'

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
                self._user_config['configMountPath'],
                self._user_config['defaultConfigMapName'])
        )

        container.env = [
            env_proj, env_creds, env_config
        ]

        # Build template spec object
        template_spec = k8s.client.V1PodSpec(containers=[
            container
        ])

        # Node selector
        template_spec.node_selector = {
            self._user_config['nodeSelectorKey']:
                self._user_config['nodeSelectorValue']
        }

        # Volume for SA secret
        volume_secret = k8s.client.V1Volume(name=volume_secret_mount_name)
        volume_secret.secret = k8s.client.V1SecretVolumeSource()
        volume_secret.secret.secret_name = sa_secret

        # Volume for config map
        volume_config_map = k8s.client.V1Volume(
            name=volume_mount_config_map_name)
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

    def _create_master_service(self, name, namespace, label):
        """
        This function a OBI master service and returns its
        public IP address and port
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.annotations = {
            self._user_config['serviceTypeMetadata']:
                self._user_config['masterServiceType']
        }
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
            self._core_client.create_namespaced_service(
                namespace, service, pretty='true')
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None

    def _create_heartbeat_service(self, name, namespace, label):
        """
        This function a OBI master service and returns its
        public IP address and port
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.namespace = namespace
        metadata.annotations = {
            self._user_config['serviceTypeMetadata']:
                self._user_config['heartbeatServiceType']
        }
        service.metadata = metadata

        # Spec object
        spec = k8s.client.V1ServiceSpec()
        spec.type = 'NodePort'
        port = k8s.client.V1ServicePort(
            port=self._user_config['defaultHeartbeatPort']
        )
        port.target_port = self._user_config['defaultHeartbeatPort']
        port.protocol = 'UDP'
        spec.ports = [
            port
        ]
        spec.selector = {
            self._user_config['defaultServiceSelectorName']: label
        }

        service.spec = spec

        try:
            srv = self._core_client.create_namespaced_service(
                namespace, service, pretty='true')
            return (self._user_config['heartbeatServiceIp'],
                    srv.spec.ports[0].node_port)
            # # Wait for the service to be fully initialized with an IP address
            # while True:
            #     status = self._core_client.read_namespaced_service_status(
            #         name, namespace)
            #     if status.status.load_balancer.ingress is not None \
            #             and status.status.load_balancer.ingress[0].ip \
            #             is not None:
            #         return (status.status.load_balancer.ingress[0].ip,
            #                 port.port)
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None

    def _create_config_map(self, name, namespace, **kwargs):
        """
        This function is used to generate a k8s config map, to be passed
        to a deployment so that the OBI master can use it to retrieve
        its configuration details.
        :param namespace:
        :return: the name of the generated config map
        """
        # Generate config map content
        cm_content = yaml.dump(kwargs, default_flow_style=False)

        # Create config map object
        config_map = k8s.client.V1ConfigMap()
        config_map.data = {
            self._user_config['defaultConfigMapName']: cm_content
        }

        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        config_map.metadata = metadata

        try:
            self._core_client.create_namespaced_config_map(
                namespace, config_map, pretty='true')
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->create_namespaced_config_map: %s\n" % e)
            return None

    def _create_predictive_component(self, name, service_name,
                                     namespace, project_id,
                                     gcs_secret_name, label):
        """
        This function creates and deploys all the k8s objects required for the
        predictive component. It then returns IP address and port information
        to contact the predictive server.
        :return:
        """
        # Create service for predictive component
        log.info('Creating predictive component service. '
                 'This operation may take a while')
        pred_host, pred_port = self._create_predictive_service(
            service_name, namespace, label)

        # Create config map to be used in the deployment
        # Generate a name for the config map
        config_map_name_pred = self._object_name_generator(
            prefix='{}-config-map-pred'.format(
                self._user_config['kubernetesNamespace']))
        self._create_config_map(
            config_map_name_pred,
            namespace,
            bucketMountPath=self._user_config['predictorBucketMountPath']
        )  # This is empty for now

        # Create deployment for predictive component
        self._create_predictive_deployment(name, namespace, project_id,
                                           gcs_secret_name,
                                           label, config_map_name_pred)

        # Return host and port to contact the predictor component
        return pred_host, pred_port

    def _create_api_component(self, deployment_name, service_name, namespace,
                              label, config_map):
        """
        Create a deployment attached to service for serving OBI's public API
        :param deployment_name:
        :param service_name:
        :param namespace:
        :param label:
        :param config_map:
        :return:
        """
        # Create service for predictive component
        log.info('Creating API component service. '
                 'This operation may take a while')
        api_host, api_port = self._create_api_service(
            service_name, namespace, label)

        # Create deployment for predictive component
        self._create_api_deployment(
            deployment_name, namespace, label, config_map)

        # Return host and port to contact the predictor component
        return api_host, api_port

    def _create_api_service(self, name, namespace, label):
        """
        Create API component external service
        :param name:
        :param namespace:
        :param label:
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.annotations = {
            self._user_config['serviceTypeMetadata']:
                self._user_config['apiServiceType']
        }
        metadata.namespace = namespace
        service.metadata = metadata

        # Spec object
        spec = k8s.client.V1ServiceSpec()
        spec.type = 'LoadBalancer'
        port = k8s.client.V1ServicePort(
            port=self._user_config['defaultApiPort']
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
            self._core_client.create_namespaced_service(
                namespace, service, pretty='true')
            while True:
                status = self._core_client.read_namespaced_service_status(
                    name, namespace)
                if status.status.load_balancer.ingress is not None \
                        and status.status.load_balancer.ingress[0].ip \
                        is not None:
                    return (status.status.load_balancer.ingress[0].ip,
                            port.port)
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None

    def _create_api_deployment(self, name, namespace, label, config_map_name):
        """
        Create API component deployment
        :param name:
        :param namespace:
        :param label:
        :param config_map:
        :return:
        """
        # Create Deployment Object
        deployment = k8s.client.V1Deployment()

        # Create metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.namespace = namespace
        metadata.annotations = {
            self._user_config['typeMetadata']:
                self._user_config['apiType'],
        }
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
            prefix="{}-api-container".format(
                self._user_config['kubernetesNamespace'])
        ))
        container.image = self._user_config['apiImage']
        container.image_pull_policy = 'Always'

        # Environment variables
        env_config = k8s.client.V1EnvVar(
            name='CONFIG_PATH', value=os.path.join(
                self._user_config['configMountPath'],
                self._user_config['defaultConfigMapName'])
        )

        container.env = [
            env_config
        ]

        # Volume mount for config map
        volume_mount_config_map_name = self._object_name_generator(
            prefix='{}-volume-mount'.format(
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

        # Node selector
        template_spec.node_selector = {
            self._user_config['nodeSelectorKey']:
                self._user_config['nodeSelectorValue']
        }

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
        spec = k8s.client.V1DeploymentSpec(
            selector=selector, template=template)
        spec.replicas = self._user_config['predictorReplicas']

        # Attach spec to deployment
        deployment.spec = spec

        # Send deployment creation request
        try:
            self._apps_client.create_namespaced_deployment(
                namespace, deployment, pretty='true')
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->create_namespaced_deployment: "
                "%s" % e)

    def _create_predictive_deployment(self, name, namespace, project_id,
                                      gcs_secret_name,
                                      label, config_map_name_pred):
        """
        Build and generate deployment for the OBI predictive component
        :return:
        """
        # Get deployment object
        deployment = self._build_predictor_deployment(name, namespace,
                                                      project_id,
                                                      gcs_secret_name, label,
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

    def _build_predictor_deployment(self, name, namespace, project_id,
                                    gcs_secret_name, label, config_map_name):
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
        metadata.annotations = {
            self._user_config['typeMetadata']:
                self._user_config['predictorType'],
            self._user_config['predictorConfigMapName']: config_map_name,
            self._user_config['predictorServiceAccountName']: gcs_secret_name
        }
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
        container.image_pull_policy = 'Always'
        container.security_context = k8s.client.V1SecurityContext(
            privileged=True)
        container.security_context.capabilities = k8s.client.V1Capabilities(
            add=['SYS_ADMIN'])

        # Mount GCS bucket to pods
        bucket_dir = None
        if self._user_config['predictorBucket'] is not None:
            bucket_dir = self._user_config['predictorBucketMountPath']
            container.lifecycle = k8s.client.V1Lifecycle()
            # Add mount operation on container start up
            container.lifecycle.post_start = k8s.client.V1Handler()
            container.lifecycle.post_start._exec = k8s.client.V1ExecAction(
                command=['sh', '-c',
                         'mkdir -p {} && gcsfuse -o nonempty {} {}'.format(
                             bucket_dir, self._user_config['predictorBucket'],
                             bucket_dir)]
            )
            # Add unmount operation on container deletion
            container.lifecycle.pre_stop = k8s.client.V1Handler()
            container.lifecycle.pre_stop._exec = k8s.client.V1ExecAction(
                command=['fusermount', '-u',
                         bucket_dir]
            )

        # Environment variables
        env_bucket = k8s.client.V1EnvVar(
            name='BUCKET_DIRECTORY', value=bucket_dir)

        env_config = k8s.client.V1EnvVar(
            name='CONFIG_PATH', value=os.path.join(
                self._user_config['configMountPath'],
                self._user_config['defaultConfigMapName'])
        )

        env_proj = k8s.client.V1EnvVar(
            name='GOOGLE_CLOUD_PROJECT', value=project_id)

        env_gac = k8s.client.V1EnvVar(
            name='GOOGLE_APPLICATION_CREDENTIALS', value=os.path.join(
                self._user_config['secretMountPath'],
                gcs_secret_name)
        )

        container.env = [
            env_bucket, env_config, env_proj, env_gac
        ]

        # Volume mount for config map
        volume_mount_config_map_name = self._object_name_generator(
            prefix='{}-volume-mount'.format(
                self._user_config['kubernetesNamespace']))
        volume_mount_config_map = k8s.client.V1VolumeMount(
            name=volume_mount_config_map_name,
            mount_path=self._user_config['configMountPath'])

        # Volume mount for secret
        volume_mount_secret_name = self._object_name_generator(
            prefix='{}-volume-mount'.format(
                self._user_config['kubernetesNamespace']))
        volume_mount_secret = k8s.client.V1VolumeMount(
            name=volume_mount_secret_name,
            mount_path=self._user_config['secretMountPath'])

        container.volume_mounts = [
            volume_mount_config_map, volume_mount_secret
        ]

        template_spec = k8s.client.V1PodSpec(containers=[
            container
        ])

        # Node selector
        template_spec.node_selector = {
            self._user_config['nodeSelectorKey']:
                self._user_config['nodeSelectorValue']
        }

        # Volume for config map
        volume_config_map = k8s.client.V1Volume(
            name=volume_mount_config_map_name)
        volume_config_map.config_map = k8s.client.V1ConfigMapVolumeSource()
        volume_config_map.config_map.name = config_map_name

        # Volume for SA secret
        volume_secret = k8s.client.V1Volume(
            name=volume_mount_secret_name)
        volume_secret.secret = k8s.client.V1SecretVolumeSource()
        volume_secret.secret.secret_name = gcs_secret_name

        template_spec.volumes = [
            volume_config_map, volume_secret
        ]
        template.spec = template_spec

        # Build spec object
        spec = k8s.client.V1DeploymentSpec(
            selector=selector, template=template)
        spec.replicas = self._user_config['predictorReplicas']

        # Attach spec to deployment
        deployment.spec = spec

        return deployment

    def _create_predictive_service(self, name, namespace, label):
        """
        This function creates the service for the predictive component
        and returns it IP address and port information
        :return:
        """
        # Create service object
        service = k8s.client.V1Service()

        # Metadata object
        metadata = k8s.client.V1ObjectMeta()
        metadata.name = name
        metadata.annotations = {
            self._user_config['serviceTypeMetadata']:
                self._user_config['heartbeatServiceType']
        }
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
            service = self._core_client.create_namespaced_service(
                namespace, service, pretty='true')
            return service.spec.cluster_ip, service.spec.ports[0].port
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->create_namespaced_service: "
                "%s\n" % e)
            return None

    def _get_namespaced_deployments(self, namespace):
        """
        Obtain a list of all the deployments belonging to the given namespace
        :param namespace:
        :return:
        """
        try:
            deployment_list = self._apps_client.list_namespaced_deployment(
                namespace)
            return deployment_list.items
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling AppsV1Api->list_namespaced_deployment"
                ": %s\n" % e)

    def _get_master_deployments(self, namespace):
        """
        Returns the IP address and port information for all the master
        services available in OBI
        by the client
        :return:
        """
        try:
            deployment_list = self._apps_client.list_namespaced_deployment(
                namespace)
            # Collect only those deployments which claim to be master
            # in their metadata
            deployments = list()
            for d in deployment_list.items:
                if self._user_config['typeMetadata'] \
                        not in d.metadata.annotations:
                    continue
                type = d.metadata.annotations[
                    self._user_config['typeMetadata']]
                if type == self._user_config['masterType']:
                    deployments.append(d)
            return deployments
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->list_namespaced_service: "
                "%s\n" % e)

    def _get_connection_information(self,
                                    namespace,
                                    name=None,
                                    deployment=None):
        """
        Given an infrastructure name, this function will return a tuple
        containing IP address and port information for connecting to
        the master of the given dataset. If the deployment argument is None
        then a deployment with the given name will be retrieved from Kubernetes
        otherwise the already available deployment object will be used
        :param name:
        :return:
        """
        # Read deployment object
        if name is None and deployment is None:
            return None

        if deployment is None and name is not None:
            # deployment = self._get_deployment_object(namespace, name)
            try:
                deployment = self._apps_client.read_namespaced_deployment(
                    name, namespace)
            except k8s.client.rest.ApiException as e:
                raise InvalidInfrastructureName(
                    'Infrastructure {} does not exist'.format(name))

        # Read service object associated to the deployment
        s_name = deployment.metadata.annotations[
            self._user_config['masterServiceName']]
        try:
            service = self._core_client.read_namespaced_service(
                s_name, namespace)
            if service.status.load_balancer.ingress is not None:
                return (service.status.load_balancer.ingress[0].ip,
                        service.spec.ports[0].port)
            raise ServiceUnreachableException('Service unreachable')
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling CoreV1Api->read_namespaced_service: "
                "%s\n" % e)
        return None

    def _get_deployment_object(self, namespace, name):
        """
        Queries Kubernetes API for obtaining a Deployment object
        :param namespace:
        :param name:
        :return:
        """
        try:
            return self._apps_client.read_namespaced_deployment(
                name, namespace)
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling AppsV1Api->read_namespaced_deployment"
                ": %s\n" % e)

    def _delete_deployment(self, namespace, name):
        """
        Deletes a given deployment
        :param namespace:
        :param name:
        :return:
        """
        try:
            self._apps_client.delete_namespaced_deployment(
                name, namespace, body=k8s.client.V1DeleteOptions())
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "AppsV1Api->delete_namespaced_deployment: %s\n" % e)

    def _delete_service(self, namespace, name):
        """
        Deletes a given service
        :param namespace:
        :param name:
        :return:
        """
        try:
            self._core_client.delete_namespaced_service(
                name, namespace, body=k8s.client.V1DeleteOptions())
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->delete_namespaced_service: %s\n" % e)

    def _delete_secret(self, namespace, name):
        """
        Deletes a given service
        :param namespace:
        :param name:
        :return:
        """
        try:
            self._core_client.delete_namespaced_secret(
                name, namespace, body=k8s.client.V1DeleteOptions())
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->delete_namespaced_secret: %s\n" % e)

    def _delete_configmap(self, namespace, name):
        """
        Deletes a given service
        :param namespace:
        :param name:
        :return:
        """
        try:
            self._core_client.delete_namespaced_config_map(
                name, namespace, body=k8s.client.V1DeleteOptions())
        except k8s.client.rest.ApiException as e:
            log.error(
                "Exception when calling "
                "CoreV1Api->delete_namespaced_config_map: %s\n" % e)
    #############
    #  END: Generic utility functions
    #############
