import docker
import roslibpy
import time
import uuid
from enum import Enum
from rigelcore.clients import DockerClient
from rigelcore.exceptions import RigelError
from rigelcore.loggers import MessageLogger
from pydantic import BaseModel, PrivateAttr, validator
from typing import Any, Dict, List, Optional, Tuple


class ConditionEnum(str, Enum):
    DIFFERENT = 'DIFFERENT'
    EQUALS = 'EQUALS'
    GREATER = 'GREATER'
    GREATER_THAN = 'GREATER_THAN'
    LESSER = 'LESSER'
    LESSER_THAN = 'LESSER_THAN'
    RECEIVED = 'RECEIVED'


class SimulationRequirement(BaseModel):

    # Required fields
    condition: ConditionEnum
    message: str
    topic: str
    value: Any

    # Optional fields
    breakpoint: bool = False
    field: Optional[str]

    @validator("field", always=True)
    def validate_date(cls, value, values) -> str:
        if value is None and values['condition'] not in [ConditionEnum.RECEIVED]:
            raise RigelError()
        return value


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
    network: Optional[str] = None
    ports: Optional[Dict[str, Optional[int]]] = None
    privileged: bool = False
    introspection: List[SimulationRequirement] = []
    volumes: Optional[List[str]] = None


class RequirementStatus(BaseModel):
    """
    Placeholder for updated information regarding a simulation requirement.

    :type calls: int
    :ivar calls: The number of received messages.
    :type satisfied: bool
    :ivar satisfied: Whether of not this simulation requirement is satisfied.
    """

    class Config:
        arbitrary_types_allowed = True

    # Required fields.
    requirement: SimulationRequirement
    listener: roslibpy.core.Topic

    # Optional fields.
    calls: int = 0
    satisfied: bool = False


class ROSBridgeClient:

    rosbridge_client: roslibpy.ros.Ros
    status: List[RequirementStatus] = []

    def __init__(self, host: str,  port: int) -> None:
        self.rosbridge_client = roslibpy.Ros(host=host, port=port)
        self.rosbridge_client.run(timeout=60)  # timeout in seconds

    def requirements_satisfied(self) -> bool:
        for requirement_status in self.status:
            if not requirement_status.satisfied:
                return False
        return True

    def breakpoint_satisfied(self) -> bool:
        for requirement_status in self.status:
            if requirement_status.requirement.breakpoint and requirement_status.satisfied:
                return True
        return False

    def get_requirements(self) -> Tuple[List[Tuple[bool, str]], List[Tuple[bool, str]]]:
        requirement_messages = []
        breakpoint_messages = []

        for requirement_status in self.status:

            if requirement_status.requirement.field:
                message = (requirement_status.satisfied, "{} ({}) - {} {} {}".format(
                    requirement_status.requirement.topic,
                    requirement_status.requirement.message,
                    requirement_status.requirement.field,
                    requirement_status.requirement.condition,
                    requirement_status.requirement.value
                ))
            else:
                message = (requirement_status.satisfied, "{} ({}) - {} {}".format(
                    requirement_status.requirement.topic,
                    requirement_status.requirement.message,
                    requirement_status.requirement.condition,
                    requirement_status.requirement.value
                ))

            if requirement_status.requirement.breakpoint:
                breakpoint_messages.append(message)
            else:
                requirement_messages.append(message)

        return requirement_messages, breakpoint_messages

    def add_requirement(self, requirement: SimulationRequirement) -> None:
        requirement_status = RequirementStatus(
            requirement=requirement,
            listener=roslibpy.Topic(self.rosbridge_client, requirement.topic, requirement.message)
        )
        requirement_status.listener.subscribe(lambda message: self.__message_handler(requirement_status, message))
        self.status.append(requirement_status)

    def __message_handler(self, requirement_status: RequirementStatus, message: Dict[str, Any]) -> None:

        requirement_status.calls = requirement_status.calls + 1

        requirement = requirement_status.requirement

        if requirement.condition == ConditionEnum.DIFFERENT:
            requirement_status.satisfied = (message[requirement.field] != requirement.value)

        elif requirement.condition == ConditionEnum.EQUALS:
            requirement_status.satisfied = (message[requirement.field] == requirement.value)

        elif requirement.condition == ConditionEnum.GREATER:
            requirement_status.satisfied = (message[requirement.field] > requirement.value)

        elif requirement.condition == ConditionEnum.GREATER_THAN:
            requirement_status.satisfied = (message[requirement.field] >= requirement.value)

        elif requirement.condition == ConditionEnum.LESSER:
            requirement_status.satisfied = (message[requirement.field] < requirement.value)

        elif requirement.condition == ConditionEnum.LESSER_THAN:
            requirement_status.satisfied = (message[requirement.field] <= requirement.value)

        elif requirement.condition == ConditionEnum.RECEIVED:
            requirement_status.satisfied = (requirement_status.calls == requirement.value)

        if requirement_status.satisfied:
            requirement_status.listener.unsubscribe()


class Plugin(BaseModel):
    """
    A plugin for Rigel to locally run a containerized ROS application.

    :type distro: string
    :param distro: The ROS distribution
    :type images: List[rigel_local_simulation_plugin.ROSPackageContainer]
    :param images: The list of containerized packages.
    :type timeout: int
    :param timeout: The maximum time (minutes) allowed for the simulation to be executed.
    """

    # List of required fields.
    distro: str
    packages: List[ROSPackageContainer]

    # List of optional fields.
    timeout: int = 600  # seconds (10 minutes)

    # List of private fields.
    _clients: List[ROSBridgeClient] = PrivateAttr()
    _docker_client: DockerClient = PrivateAttr()
    _message_logger: MessageLogger = PrivateAttr()
    _network_name: str = PrivateAttr()
    _simulation_uuid: str = PrivateAttr()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._clients = []
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
                self._message_logger.info("\t- Connected to containerized ROS bridge.")

                for requirement in package.introspection:
                    rosbridge_client.add_requirement(requirement)
                self._clients.append(rosbridge_client)

    def requirements_satisfied(self) -> bool:
        """
        Detect whether or not all simulation requirements were satisfied.

        :rtype: bool
        :return: True if all simulation requirements were satisfied. False otherwise.
        """
        if not self._clients:  # allow for timeout-based simulations
            return False

        for client in self._clients:
            if not client.requirements_satisfied():
                return False
        return True

    def breakpoint_satisfied(self) -> bool:
        """
        Detect whether or not a simulation breakpoint was satisfied.

        :rtype: bool
        :return: True if at least a simulation breakpoint was satisfied. False otherwise.
        """
        if not self._clients:  # allow for timeout-based simulations
            return False

        for client in self._clients:
            if client.breakpoint_satisfied():
                return True
        return False

    def get_requirements(self) -> Tuple[List[Tuple[bool, str]], List[Tuple[bool, str]]]:
        """
        Fetch information about all the simulation requirements and their current status.

        :type: Tuple(List[Tuple(bool, str)], List[Tuple(bool, str)])
        :return: The list of requirement status and a list of breakpoint status.
        """
        requirement_messages = []
        breakpoint_messages = []

        for client in self._clients:
            requirements, breakpoints = client.get_requirements()
            requirement_messages = requirement_messages + requirements
            breakpoint_messages = breakpoint_messages + breakpoints

        return requirement_messages, breakpoint_messages

    def bringup_simulation_infrastructure(self) -> None:
        """
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

        # Bringup all ROS nodes
        self.bringup_ros_nodes()

    def destroy_simulation_infrastructure(self) -> None:
        """
        Remove simulation network and all containers associated with a given simulation.
        """
        # Remove containers
        for package in self.packages:
            self._docker_client.remove_container(package.name)
            self._message_logger.info(f"Removed Docker container '{package.name}'")

        # Remove simulation network
        self.remove_simulation_network()
        self._message_logger.info(f"Removed Docker network '{self._network_name}'.")

    def run(self) -> None:
        """
        Plugin entrypoint.
        """
        self.bringup_simulation_infrastructure()

        # Execution loop - block wait until all requirements to be satisfied or timeout is reached
        self._message_logger.warning("Simulation started.")
        initial_time = time.time()
        while True:  # implement timeout mechanism
            passed_time = time.time() - initial_time
            if passed_time > self.timeout:
                self._message_logger.error(f"Timeout ({passed_time}s). Simulation requirements were not satisfied on time.")
                break
            elif self.breakpoint_satisfied():
                self._message_logger.error(f"A simulation breakpoint was satisfied at {passed_time}s. Terminating.")
                break
            elif self.requirements_satisfied():
                self._message_logger.info(f"All requirements satisfied. Simulation concluded with success on {passed_time}s.")
                break

        self.stop()

        requirement_messages, breakpoint_messages = self.get_requirements()
        if requirement_messages:
            print('REQUIREMENTS:')
            for message in requirement_messages:
                status, entry = message
                print(f" - {entry}\t...\t{'SUCCESS' if status else 'FAILURE'}")
            print()
        if breakpoint_messages:
            print('BREAKPOINTS:')
            for message in breakpoint_messages:
                status, entry = message
                print(f" - {entry}\t...\t{'SUCCESS' if status else 'FAILURE'}")
            print()

    def stop(self) -> None:
        """
        Plugin graceful closing mechanism.
        """
        self.destroy_simulation_infrastructure()
