import docker
import uuid
from rigelcore.clients import (
    DockerClient,
    ROSBridgeClient
)
from rigelcore.loggers import MessageLogger
from rigelcore.simulations import SimulationRequirementsManager
from pydantic import BaseModel, PrivateAttr
from typing import Any, Dict, List, Optional


class ROSPackageContainer(BaseModel):
    """
    A placeholder for information regarding a containerized ROS package.

    :type name: string
    :param name: The Docker container name.
    :type image: string
    :param name: The Docker image.
    :type command: Optional[str]
    :param command: The command to be executed inside the container.
    :type environment: Optional[List[str]]
    :param environment: The list of environment variables to set inside the container.
    :type instrospection: List[SimulationRequirement].
    :param instrospection: The list of conditions that must be fulfilled.
    :type network: Optional[str]
    :param network: The name of the network to connect the container to.
    :type ports: Optional[Dict[str, Optional[int]]]
    :param ports: The container ports to expose.
    :type volumes: Optional[List[str]]
    :param volumes: The list of volumes to be mounted inside the container.
    """
    # Required fields.
    name: str
    image: str

    # Optional fields.
    command: Optional[str] = None
    environment: Optional[List[str]] = []
    introspection: bool = False
    network: Optional[str] = None
    ports: Optional[Dict[str, Optional[int]]] = None
    privileged: bool = False
    volumes: Optional[List[str]] = None


class Plugin(BaseModel):
    """
    A plugin for Rigel to locally run a containerized ROS application.

    :type distro: string
    :param distro: The ROS distribution
    :type images: List[rigel_local_simulation_plugin.ROSPackageContainer]
    :param images: The list of containerized packages.
    """

    # List of required fields.
    distro: str
    packages: List[ROSPackageContainer]

    # List of private fields.
    _docker_client: DockerClient = PrivateAttr()
    _message_logger: MessageLogger = PrivateAttr()
    _network_name: str = PrivateAttr()
    _requirements_manager: SimulationRequirementsManager = PrivateAttr()
    _simulation_uuid: str = PrivateAttr()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args[1:], **kwargs)
        self._requirements_manager = args[0]
        self._docker_client = DockerClient()
        self._message_logger = MessageLogger()
        self._simulation_uuid = str(uuid.uuid1())
        self._network_name = f'rigel-simulation-{self._simulation_uuid}'

    def create_simulation_network(self) -> None:
        """
        Create dedicated Docker network created for a simulation.
        """
        self._docker_client.create_network(self._network_name, 'bridge')

    def remove_simulation_network(self) -> None:
        """
        Remove dedicated Docker network created for a simulation.
        """
        self._docker_client.remove_network(self._network_name)

    def run_ros_package_container(self, package: ROSPackageContainer) -> docker.models.containers.Container:
        """
        Launch a single containerized ROS node.

        :type package: rigel_local_simulation_plugin.ROSPackageContainer
        :param package: Information about the ROS package container.

        :rtype: docker.models.containers.Container
        :return: The Docker container serving as ROS master.
        """
        self._docker_client.run_container(
            package.name,
            package.image,
            command=package.command,
            detach=True,
            environment=package.environment,
            hostname=package.name,
            network=self._network_name,
            privileged=package.privileged,
            volumes=package.volumes

        )
        self._docker_client.wait_for_container_status(package.name, 'running')
        return self._docker_client.get_container(package.name)  # this call to get_container ensures updated container data

    def bringup_ros_nodes(self) -> None:
        """
        Launch all containerized ROS nodes required for a given simulation.
        """
        # Start containerize ROS application
        for package in self.packages:

            ros_common_env_variables = ['ROS_MASTER_URI=http://master:11311', f'ROS_HOSTNAME={package.name}']

            # Ensure that all ROS nodes connect to the same ROS master node
            package.environment = package.environment + ros_common_env_variables

            node_container = self.run_ros_package_container(package)
            node_container_addr = node_container.attrs['NetworkSettings']['Networks'][self._network_name]['IPAddress']
            self._message_logger.info(f"Created container '{package.name}' ({node_container_addr})")

            if package.introspection:

                # Connect to ROS bridge inside containter
                rosbridge_client = ROSBridgeClient(node_container_addr, 9090)
                self._message_logger.info(f"Connected to ROS bridge server at '{node_container_addr}:9090'")

                self._requirements_manager.connect_requirements_to_rosbridge(rosbridge_client)

    def run(self) -> None:
        """
        Plugin entrypoint.
        Create simulation network and all containers required for a given simulation.
        """
        # Create Docker network for entire simulation
        self.create_simulation_network()
        self._message_logger.info(f"Created Docker network {self._network_name}")

        # Add container with ROS master node
        self.packages.insert(0, ROSPackageContainer(
            name='master',
            image=f'ros:{self.distro}',
            command='roscore'
        ))

        # Bringup all remaining ROS nodes
        self.bringup_ros_nodes()

    def stop(self) -> None:
        """
        Plugin graceful closing mechanism.
        """
        # Remove containers
        for package in self.packages:
            self._docker_client.remove_container(package.name)
            self._message_logger.info(f"Removed Docker container '{package.name}'")

        # Remove simulation network
        self.remove_simulation_network()
        self._message_logger.info(f"Removed Docker network '{self._network_name}'.")
