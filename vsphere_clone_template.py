#!/usr/bin/python

import atexit
import time
import datetime
from datetime import datetime, timedelta
import random
import operator
import itertools
import json
from contextlib import contextmanager

HAS_PYVMOMI = False
try:
    from pyVmomi import vim
    from pyVmomi import vmodl
    from pyVim.connect import SmartConnect, Disconnect
    HAS_PYVMOMI = True

except ImportError as e:
    pass


class VsphereHelpers(object):
    @staticmethod
    def create_storage_selection_spec(vi_content, datastore_cluster, desired_disks):
        os_datastore_cluster = VsphereHelpers.get_obj(vi_content, [vim.StoragePod], datastore_cluster)
        podsel = vim.storageDrs.PodSelectionSpec()
        podsel.storagePod = os_datastore_cluster
        if len(desired_disks) > 0:
            pod_configs = []
            for key, group in itertools.groupby(sorted(desired_disks, key=operator.itemgetter("datastore_cluster")), lambda y: y["datastore_cluster"]):
                pod_config = vim.VmPodConfigForPlacement()
                current_dsc = VsphereHelpers.get_obj(vi_content, [vim.StoragePod], key)
                pod_config.storagePod = current_dsc
                dsc_disks = []
                for disk in group:
                    disk_type = disk["type"]
                    disk_backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
                    if disk_type == "thin":
                        disk_backing.thinProvisioned = True
                    if disk_type == "eager":
                        disk_backing.set_element_eagerlyScrub = True
                    disk_backing.diskMode = "persistent"
                    disk_locator = vim.PodDiskLocator()
                    disk_locator.diskId = int(disk["vsphere_key"])
                    disk_locator.diskBackingInfo = disk_backing
                    dsc_disks.append(disk_locator)
                pod_config.disk = dsc_disks
                pod_configs.append(pod_config)
            podsel.initialVmConfig = pod_configs
        return podsel

    @staticmethod
    def create_storage_placement_spec(guest_name, guest_folder, storage_selection_spec, template, clone_spec, resource_pool):
        storage_spec = vim.storageDrs.StoragePlacementSpec()
        storage_spec.podSelectionSpec = storage_selection_spec
        storage_spec.type = "clone"
        storage_spec.folder = guest_folder
        storage_spec.resourcePool = resource_pool
        storage_spec.vm = template
        storage_spec.cloneName = guest_name
        storage_spec.cloneSpec = clone_spec
        return storage_spec

    @staticmethod
    def create_clone_spec(relocate_spec, config_spec, customization_spec=None, isTemplate=False):
        # Clone spec
        clonespec = vim.vm.CloneSpec()
        clonespec.location = relocate_spec
        clonespec.config = config_spec
        if customization_spec is not None:
            clonespec.customization = customization_spec
        clonespec.powerOn = not isTemplate
        clonespec.template = isTemplate
        return clonespec

    @staticmethod
    def create_config_spec(memory_in_mb, cpu, devices):
        conf_spec = vim.vm.ConfigSpec()
        if memory_in_mb is not None:
            conf_spec.memoryMB = int(memory_in_mb)
        if cpu is not None:
            conf_spec.numCPUs = int(cpu)
        conf_spec.memoryHotAddEnabled = True
        conf_spec.cpuHotAddEnabled = True
        conf_spec.deviceChange = devices
        conf_spec.memoryReservationLockedToMax = False
        conf_spec.tools = VsphereHelpers.create_tools_spec()
        return conf_spec

    @staticmethod
    def create_tools_spec():
        tools_spec = vim.vm.ToolsConfigInfo()
        tools_spec.toolsUpgradePolicy = "manual"
        return tools_spec

    @staticmethod
    def create_relocation_spec(resource_pool, datastore):
        relocate = vim.vm.RelocateSpec()
        relocate.pool = resource_pool
        relocate.datastore = datastore
        return relocate

    @staticmethod
    def wait_task(task, actionName='job', hideResult=False, timeout=600):
        start_time = datetime.now()
        while task.info.state == vim.TaskInfo.State.running:
            time.sleep(2)
            current_time = datetime.now()
            if (current_time - start_time).seconds > timeout:
                raise Exception("vCenter Timeout: Task took longer than %s seconds to complete." % timeout)

        if task.info.state == vim.TaskInfo.State.success:
           if task.info.result is not None and not hideResult:
              out = '%s completed successfully, result: %s' % (actionName, task.info.result)
           else:
              out = '%s completed successfully.' % actionName
        else:
           out = '%s did not complete successfully: %s' % (actionName, task.info.error)
           print out
           raise Exception(task.info.error) # should be a Fault... check XXX

        # may not always be applicable, but can't hurt.
        return task.info.result

    @staticmethod
    def collect_properties(service_instance, view_ref, obj_type, path_set=None, include_mors=False):
        """
        Collect properties for managed objects from a view ref
        Check the vSphere API documentation for example on retrieving
        object properties:
            - http://goo.gl/erbFDz
        Args:
            si          (ServiceInstance): ServiceInstance connection
            view_ref (pyVmomi.vim.view.*): Starting point of inventory navigation
            obj_type      (pyVmomi.vim.*): Type of managed object
            path_set               (list): List of properties to retrieve
            include_mors           (bool): If True include the managed objects
                                           refs in the result
        Returns:
            A list of properties for the managed objects
        """
        collector = service_instance.content.propertyCollector

        # Create object specification to define the starting point of
        # inventory navigation
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
        obj_spec.obj = view_ref
        obj_spec.skip = True

        # Create a traversal specification to identify the path for collection
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
        traversal_spec.name = 'traverseEntities'
        traversal_spec.path = 'view'
        traversal_spec.skip = False
        traversal_spec.type = view_ref.__class__
        obj_spec.selectSet = [traversal_spec]

        # Identify the properties to the retrieved
        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.type = obj_type

        if not path_set:
            property_spec.all = True

        property_spec.pathSet = path_set

        # Add the object and property specification to the
        # property filter specification
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.objectSet = [obj_spec]
        filter_spec.propSet = [property_spec]

        # Retrieve properties
        props = collector.RetrieveContents([filter_spec])

        data = []
        for obj in props:
            properties = {}
            for prop in obj.propSet:
                properties[prop.name] = prop.val

            if include_mors:
                properties['obj'] = obj.obj

            data.append(properties)
        return data

    @staticmethod
    def get_container_view(service_instance, obj_type, container=None):
        """
        Get a vSphere Container View reference to all objects of type 'obj_type'
        It is up to the caller to take care of destroying the View when no longer
        needed.
        Args:
            obj_type (list): A list of managed object types
        Returns:
            A container view ref to the discovered managed objects
        """
        if not container:
            container = service_instance.content.rootFolder

        view_ref = service_instance.content.viewManager.CreateContainerView(
            container=container,
            type=obj_type,
            recursive=True
        )
        return view_ref

    @staticmethod
    def get_vm(service_instance, vm_name):
        view = VsphereHelpers.get_container_view(service_instance,
                                       obj_type=[vim.VirtualMachine])

        vm_data = VsphereHelpers.collect_properties(service_instance, view_ref=view,
                                          obj_type=vim.VirtualMachine,
                                          path_set=["name"],
                                          include_mors=True)

        vms = [x['obj'] for x in vm_data if x["name"] == vm_name]

        return vms

    @staticmethod
    def get_obj(content, vimtype, name):
        obj = None
        container = content.viewManager.CreateContainerView(
            content.rootFolder, vimtype, True)
        for c in container.view:
            if c.name == name:
                obj = c
                break
        return obj


class MediaHelpers(object):
    @staticmethod
    def get_media_drive(vsphere, template):
        if template is not None:
            cd = [x for x in template.config.hardware.device if type(x) == vim.vm.device.VirtualCdrom]
            if len(cd) > 0:
                media_device = vim.vm.device.VirtualDeviceSpec()
                media_device.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                media_device.device = cd[0]
                media_device.device.connectable.startConnected = False
                media_device.device.backing = vim.vm.device.VirtualCdrom.IsoBackingInfo()
                return media_device
            else:
                return None

class NetworkHelpers(object):
    @staticmethod
    def get_network(service_instance, network_name):
        view = VsphereHelpers.get_container_view(service_instance, obj_type=[vim.Network])

        network_data = VsphereHelpers.collect_properties(service_instance, view_ref=view,
                                                         obj_type=vim.Network,
                                                         path_set=["name"], include_mors=True)

        if network_name is not None:
            networks = [x['obj'] for x in network_data if x["name"] == network_name]
            return networks
        else:
            return network_data

    @staticmethod
    def get_cluster_network(cluster, network_name):
        if network_name is not None:
            networks = [x for x in cluster.network if x["name"] == network_name]
            return networks

    @staticmethod
    def create_nic_spec(network, nic_type="vmxnet3"):
        nicspec = vim.vm.device.VirtualDeviceSpec()
        nicspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add

        if nic_type == "e1000":
            nic_controller = vim.vm.device.VirtualE1000()
        elif nic_type == "e1000e":
            nic_controller = vim.vm.device.VirtualE1000e()
        elif nic_type == "pcnet32":
            nic_controller = vim.vm.device.VirtualPCNet32()
        elif nic_type == "vmxnet":
            nic_controller = vim.vm.device.VirtualVmxnet()
        elif nic_type == "vmxnet2":
            nic_controller = vim.vm.device.VirtualVmxnet2()
        else:
            nic_controller = vim.vm.device.VirtualVmxnet3()

        nicspec.device = nic_controller
        if hasattr(network, "key"):
            nicspec.device.backing = vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
            dvs_port_connection = vim.dvs.PortConnection()
            dvs_port_connection.portgroupKey = network.key
            dvs_port_connection.switchUuid = network.config.distributedVirtualSwitch.uuid
            nicspec.device.backing.port = dvs_port_connection
        else:
          nicspec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
          nicspec.device.backing.network = network
          nicspec.device.backing.deviceName = network.name

        nicspec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
        nicspec.device.connectable.startConnected = True
        nicspec.device.connectable.allowGuestControl = True
        return nicspec


class DiskHelpers(object):
    @staticmethod
    def create_disk_ctrl_spec(type="paravirtual", bus_number=1, control_key=1, scsi_sharing=vim.vm.device.VirtualSCSIController.Sharing.noSharing):
        control_spec = vim.vm.device.VirtualDeviceSpec()
        control_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add

        # Set Controller type. Defaults to LSILogic
        if type == "paravirtual":
            controller = vim.vm.device.ParaVirtualSCSIController()
        elif type == "lsi_sas":
            controller = vim.vm.device.VirtualLsiLogicSASController()
        elif type == "bus_logic":
            controller = vim.vm.device.VirtualBusLogicController()
        else:
            controller = vim.vm.device.VirtualLsiLogicController()

        controller.sharedBus = scsi_sharing
        controller.key = control_key
        controller.busNumber = bus_number
        control_spec.device = controller
        return control_spec


    @staticmethod
    def create_disk_spec(datastore=None, disk_type="thick", size=200000, disk_control_key=1, disk_number=0, disk_key=1):
        diskspec = vim.vm.device.VirtualDeviceSpec()
        diskspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        diskspec.fileOperation = vim.vm.device.VirtualDeviceSpec.FileOperation.create

        disk_backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        if disk_type == "thin":
            disk_backing.thinProvisioned = True
        if disk_type == "eager":
            disk_backing.set_element_eagerlyScrub = True

        # TODO: Pass this in as a param?
        disk_backing.diskMode = "persistent"

        if datastore is not None:
            disk_backing.fileName = "[%s]" % datastore.name
            disk_backing.datastore = datastore

        disk = vim.vm.device.VirtualDisk()
        disk.key = int(disk_key)
        disk.backing = disk_backing
        disk.capacityInKB = int(size) * 1024 * 1024
        disk.controllerKey = int(disk_control_key)
        disk.unitNumber = int(disk_number)
        diskspec.device = disk

        return diskspec

    @staticmethod
    def get_defined_disk_info(disk_key, disk):
        datastore = None
        disk_size = None
        disk_type = None
        mount_point = None
        fs_type = None
        datastore_cluster = None
        # Make datastore optional, if not specified, we go and find one
        if "datastore" in disk:
            datastore = disk["datastore"]
        elif "datastore_cluster" in disk:
            datastore = None
            datastore_cluster = disk["datastore_cluster"]
        else:
            raise Exception("Did not find datastore or datastore_cluster defined in disk: %s" % json.dumps(disk))

        if "size_gb" in disk:
            disk_size = int(disk["size_gb"])
        else:
            raise Exception("Size not specified for disk: %s" % json.dumps(disk))

        if "type" in disk:
            disk_type = disk["type"]
        else:
            raise Exception("type not specified for disk: %s" % json.dumps(disk))

        if "mount_point" in disk:
            mount_point = disk["mount_point"]
        else:
            raise Exception("mount_point not specified for disk: %s" % json.dumps(disk))

        if "fs_type" in disk:
            fs_type = disk["fs_type"]
        else:
            raise Exception("fs_type not specified for disk: %s" % json.dumps(disk))

        return {"datastore": datastore,
                "datastore_cluster": datastore_cluster,
                "size_gb": disk_size,
                "type": disk_type,
                "label": disk_key,
                "mount_point": mount_point,
                "fs_type": fs_type
        }


class CustomizationHelpers(object):
    @staticmethod
    def create_adapter_mappings(desired_networks):
        guest_adapters = []
        for network in desired_networks:
            guest_map = vim.vm.customization.AdapterMapping()
            guest_map.adapter = vim.vm.customization.IPSettings()
            guest_map.adapter.ip = vim.vm.customization.FixedIp()
            if "ip" in network and "netmask" in network:
                guest_map.adapter.ip.ipAddress = network["ip"]
                guest_map.adapter.subnetMask = network["netmask"]
                if "position" in network and int(network["position"]) == 0:
                    if "gateway" in network:
                        guest_map.adapter.gateway = [network["gateway"]]
            if "domain" in network:
                guest_map.adapter.dnsDomain = network["domain"]

            guest_adapters.append(guest_map)
        return guest_adapters

    @staticmethod
    def create_linux_customization_spec(desired_networks, domain, guest_name):

        # DNS settings
        globalip = vim.vm.customization.GlobalIPSettings()
        globalip.dnsServerList = list(set([x for y in [n["dns"] for n in desired_networks] for x in y]))
        globalip.dnsSuffixList = [domain]

        # Hostname settings
        ident = vim.vm.customization.LinuxPrep()
        ident.domain = domain
        ident.hostName = vim.vm.customization.FixedName()
        ident.hostName.name = guest_name

        customspec = vim.vm.customization.Specification()
        customspec.nicSettingMap = CustomizationHelpers.create_adapter_mappings(desired_networks)
        customspec.globalIPSettings = globalip
        customspec.identity = ident
        return customspec

    @staticmethod
    def create_windows_customization_spec(desired_networks, guest_name, product_id, org_name, provision_user):
         # DNS settings
        globalip = vim.vm.customization.GlobalIPSettings()
        globalip.dnsServerList = list(set([x for y in [n["dns"] for n in desired_networks] for x in y]))
        globalip.dnsSuffixList = []

        # Windows Options Settings
        win_options = vim.vm.customization.WinOptions()
        win_options.changeSID = True
        win_options.deleteAccounts = False

        # GUI Unattended Spec
        gui_unattend = vim.vm.customization.GuiUnattended()
        gui_unattend.autoLogon = False
        gui_unattend.autoLogonCount = 0
        gui_unattend.timeZone = 85

        # # Hostname Setting
        fixedName = vim.vm.customization.FixedName()
        fixedName.name = guest_name

        userData = vim.vm.customization.UserData()
        userData.computerName = fixedName
        if provision_user is not None:
            userData.fullName = provision_user
        else:
            userData.fullName = "Ansible Provisioned"
        if org_name is not None:
            userData.orgName = org_name
        else:
            userData.orgName = "Ansible Provisioned"

        if product_id is not None:
            userData.productId = product_id

        sysprep = vim.vm.customization.Sysprep()
        sysprep.guiUnattended = gui_unattend
        sysprep.identification = vim.vm.customization.Identification()
        sysprep.userData = userData

        customspec = vim.vm.customization.Specification()
        customspec.identity = sysprep
        customspec.options = win_options
        customspec.nicSettingMap = CustomizationHelpers.create_adapter_mappings(desired_networks)
        customspec.globalIPSettings = globalip

        return customspec


class FolderHelpers(object):
    @staticmethod
    def get_congo_folder(vsphere, folder_structure, template_vm):
        if folder_structure is None or len(folder_structure) == 0:
            return template_vm.parent

        folder_structure = [x for x in folder_structure if x is not None and x != ""]

        datacenter_folder = FolderHelpers.get_datacenter_folder(template_vm)
        if datacenter_folder is not None:
            folder_objects = FolderHelpers.get_folder_objects(vsphere, datacenter_folder["dc"])
            folder_mor, folder_children = FolderHelpers.find_folder(vsphere, folder_structure, folder_objects, datacenter_folder)
            return folder_mor


    @staticmethod
    def create_folder(root_folder, new_name):
        return root_folder.CreateFolder(new_name)

    @staticmethod
    def get_folder_objects(vsphere, root_folder):
        view = VsphereHelpers.get_container_view(vsphere,
                                       obj_type=[vim.Folder], container=root_folder)

        folders = VsphereHelpers.collect_properties(vsphere,
                                                    view_ref=view,
                                                    obj_type=vim.Folder,
                                                    path_set=['name', 'childEntity', 'parent'],
                                                    include_mors=True)

        folder_objects = [{"folder": x["obj"],
                           "child": x["childEntity"],
                           "name": x["name"],
                           "parent": x["parent"]} for x in folders if type(x["obj"]) is vim.Folder]

        vm_folders = [x for x in folder_objects if len(x["child"]) == 0 or type(x["child"][0]) is vim.Folder or type(x["child"][0]) is vim.VirtualMachine]
        # folder_objects = [{"folder": x.Obj,
        #           "child": [y.Val.ManagedObjectReference for y in x.PropSet if y.Name == "childEntity"][0],
        #           "name": [y.Val for y in x.PropSet if y.Name == "name"][0],
        #           "parent": [y.Val for y in x.PropSet if y.Name == "parent"]
        #          } for x in folders]

        return vm_folders

    @staticmethod
    def get_datacenter_folder(template_vm):
        parent = template_vm.parent
        while type(parent) is not vim.Datacenter:
            if parent is None or not hasattr(parent, "parent"):
                break
            parent = parent.parent
            continue

        if parent is not None:
            return {"dc": parent, "folder": parent.vmFolder, "name": parent.name}
        else:
            return None

    @staticmethod
    def find_folder(vsphere, folder_structure, folder_objects, dc_folder):
        folder_mor = None
        folder_children = []

        folder_levels = [[x for x in folder_objects if x["name"] == folder] for folder in folder_structure]

        root_tree = pytree("root", None)

        i = 0
        current_tree = None
        parent_tree = None
        for folder_list in folder_levels:
            for folder in folder_list:
                current_tree = pytree(folder["folder"], folder["child"], meta_data={"name": folder["name"], "parent": folder["parent"]})
                if i == 0:
                    root_tree.add_leaf(current_tree)
                else:
                    if folder_structure[i] == folder["name"]:
                        parent_tree = pytree.search_leaves_extra_data(root_tree, folder["folder"])
                        if parent_tree is not None:
                            parent_tree.add_leaf(current_tree)
                            parent_tree = None
            i += 1
        #remove Non DC Leaves
        # [root_tree.leaves.remove(y) for y in [x for x in root_tree.leaves if dc_folder["folder"] != x.meta_data["parent"]]]
        deep_leaves = pytree.get_deepest_leaves(root_tree)

        if not isinstance(deep_leaves, list):
            if isinstance(deep_leaves, pytree):
                if str(deep_leaves.name) == "root":
                    deep_leaves = []
                else:
                    deep_leaves = [deep_leaves]
            else:
                deep_leaves = [deep_leaves]

        deep_leaves_details = [pytree.get_repr(x) for x in deep_leaves]
        remove_keys = []
        if len(deep_leaves) == len(deep_leaves_details):
            for key, structure in enumerate(deep_leaves_details):
                if structure == ",".join(folder_structure):
                    folder_mor = deep_leaves[key].name
                    folder_children = deep_leaves[key].extra_data
                else:
                    remove_keys.append(deep_leaves[key])

        for key in remove_keys:
            deep_leaves.remove(key)

        if len(deep_leaves) > 1:
            raise Exception("Found more than one matching folder for structure: %s. Be more unique!" % json.dumps(folder_structure))

        if len(deep_leaves) < 1:
            # See if we can go up a level; and create the folder
            if len(folder_structure) > 1:
                root_folder, folder_children = FolderHelpers.find_folder(vsphere, folder_structure[:-1], folder_objects, dc_folder)
                FolderHelpers.create_folder(root_folder, folder_structure[-1])
                return FolderHelpers.find_folder(vsphere, folder_structure, FolderHelpers.get_folder_objects(vsphere, dc_folder["folder"]), dc_folder)
            elif len(folder_structure) == 1:
                FolderHelpers.create_folder(dc_folder["folder"], folder_structure[0])
                return FolderHelpers.find_folder(vsphere, folder_structure, FolderHelpers.get_folder_objects(vsphere, dc_folder["folder"]), dc_folder)
            else:
                raise Exception("Could not find any matching folder for structure: %s." % json.dumps(folder_structure))

        return folder_mor, folder_children


class pytree:
    def __init__(self, name, extra_data, meta_data=None):
        self.name = name
        self.extra_data = extra_data
        self.meta_data = meta_data
        self.parent = None
        self.leaves = []
        self.root = None

    def set_parent(self, parent):
        self.parent = parent

    def add_leaf(self, node):
        node.set_parent(self)
        parent = node.parent
        root = None
        while (parent is not None):
            root = parent
            parent = parent.parent

        node.set_root(root)
        self.leaves.append(node)

    def set_root(self, root):
        self.root = root

    @staticmethod
    def search_leaves_extra_data(tree, data):
        node_with_data = None
        for node in tree.leaves:
            if data in node.extra_data:
                node_with_data = node
                break
            else:
                node_with_data = pytree.search_leaves_extra_data(node, data)
                if node_with_data is not None:
                    break
        return node_with_data

    @staticmethod
    def get_deepest_leaves(tree):
        if len(tree.leaves) > 0:
            leaves = [pytree.get_deepest_leaves(x) for x in tree.leaves]
            if len(leaves) > 1:
                return leaves
            elif len(leaves) == 1:
                return leaves[0]
            else:
                return None
        else:
            return tree

    @staticmethod
    def get_repr(tree):
        if not isinstance(tree, pytree):
            return tree

        rep_arr = []
        if tree.meta_data is not None:
            rep_arr.append(tree.meta_data["name"])

        parent = tree.parent
        while parent is not None:
            if parent.meta_data is not None:
                rep_arr.append(parent.meta_data["name"])
            parent = parent.parent

        return ",".join(rep_arr[::-1])


def deploy_template(vsphere, vi_content, guest, template_src, cluster_name, domain, vm_cpu, vm_memory_mb, os_family, vm_disk, vm_nic, windows_product_id=None, windows_org_name=None, windows_provision_user=None, is_template=False, folder_structure=None):
    template_vm_arr = VsphereHelpers.get_vm(vsphere, template_src)
    if len(template_vm_arr) < 1:
        raise Exception("Could not find VM Template: %s" % template_src)
    
    template_vm = template_vm_arr[0]    
    cluster = VsphereHelpers.get_obj(vi_content, [vim.ClusterComputeResource], cluster_name)
    resource_pool = cluster.resourcePool

    storage_select_spec = None
    datastore = None

    #Define devices
    devices = []

    #CDROM Setup
    media_drive = MediaHelpers.get_media_drive(vsphere, template_vm)
    if media_drive is not None:
        devices.append(media_drive)

    #NIC Setup
    if vm_nic is not None:
        desired_networks = sorted(vm_nic.values(), key=operator.itemgetter("position"))
        for net in desired_networks:
            potential_networks = [x for x in NetworkHelpers.get_network(vsphere, net["name"]) if x in cluster.network]
            if len(potential_networks) == 1:
                devices.append(NetworkHelpers.create_nic_spec(potential_networks[0]))
            elif len(potential_networks) == 0:
                raise Exception("Could not find network named: %s attached to cluster: %s" % (net["name"], cluster_name))
            else:
                raise Exception("Found more than one network named: %s attached to cluster: %s" % (net["name"], cluster_name))

    #Datastore Selection
    os_disk = None
    if "os_disk" in vm_disk:
        os_disk = vm_disk["os_disk"]
        if "datastore" not in os_disk:
            os_disk["datastore"] = None

    tmp_disk = vm_disk.copy()
    if "os_disk" in vm_disk:
        del tmp_disk["os_disk"]

    desired_disk_details = []
    if not is_template:
        if len(tmp_disk.keys()) > 0:
            devices.append(DiskHelpers.create_disk_ctrl_spec())
            vm_disk_count = len(tmp_disk) - 1
            desired_disk_details = [DiskHelpers.get_defined_disk_info(k,tmp_disk[k]) for k in sorted(tmp_disk)]

            for k, disk in enumerate(desired_disk_details):
                disk["drive_id"] = str(k+1)
                disk["vsphere_key"] = -(k+1)
                devices.append(DiskHelpers.create_disk_spec(datastore=disk["datastore"], disk_type=disk["type"], size=disk["size_gb"], disk_number=disk["drive_id"], disk_key=disk["vsphere_key"]))


    if os_disk is not None:
        if "datastore_cluster" in os_disk:
            storage_select_spec = VsphereHelpers.create_storage_selection_spec(vi_content, os_disk["datastore_cluster"], desired_disk_details)
        elif "datastore" in os_disk:
            datastore = VsphereHelpers.get_obj(vi_content, [vim.Datastore], os_disk["datastore"])

    relocate_spec = VsphereHelpers.create_relocation_spec(resource_pool, datastore)

    config_spec = VsphereHelpers.create_config_spec(vm_memory_mb, vm_cpu, devices)

    # Not going to customize templates
    if not is_template:
        guest_family = os_family.lower()
        if "windows" not in guest_family:
            customization_spec = CustomizationHelpers.create_linux_customization_spec(desired_networks, domain, guest)
        else:
            customization_spec = CustomizationHelpers.create_windows_customization_spec(desired_networks, guest, windows_product_id, windows_org_name, windows_provision_user)
    else:
        customization_spec = None

    clone_spec = VsphereHelpers.create_clone_spec(relocate_spec, config_spec, customization_spec, is_template)
    # Find folder logic needs to be included
    folder = FolderHelpers.get_congo_folder(vsphere, folder_structure, template_vm)

    if storage_select_spec is not None:
        storage_placement_spec = VsphereHelpers.create_storage_placement_spec(guest, folder, storage_select_spec, template_vm, clone_spec, resource_pool)
        errors = []
        clone_attempt = 0
        clone_result = None
        while clone_attempt < 3:
            clone_attempt += 1
            try:
                clone_result = _recommend_and_clone(vi_content, storage_placement_spec, vm_disk, desired_disk_details, is_template)
                break
            except Exception as err:
                if hasattr(err, "message") and err.message != "":
                    message = str(err.message)
                elif hasattr(err, "msg"):
                    message = str(err.msg)
                else:
                    message = str(err)

                if "DuplicateName" in message and is_template:
                    # def clone_result():
                    #     vm = guest
                    clone_result = lambda: None
                    setattr(clone_result, "vm", guest)
                    time.sleep(360)
                    break
                if message not in errors:
                    errors.append(message)

                time.sleep(60)
                clone_result = None

        if clone_result is not None and hasattr(clone_result, "vm"):
            return {"vm": guest, "disk": _convert_disk_list_to_dict(desired_disk_details)}
        else:
            raise Exception("Could not clone VM after %s attempts: %s" % (clone_attempt, json.dumps(errors)))
    else:
        # fire the clone task
        task = template_vm.Clone(folder=folder, name=guest, spec=clonespec)
        result = VsphereHelpers.wait_task(task, 'VM clone task')
        return {"vm": guest, "disk": _convert_disk_list_to_dict(desired_disk_details)}


def _recommend_and_clone(vi_content, storage_placement_spec, vm_disk, desired_disk_details, is_template):
    rec_result = vi_content.storageResourceManager.RecommendDatastores(storage_placement_spec)
    if not is_template:
        needed_rec_length = len(set([x["datastore_cluster"] for x in vm_disk.values()]))
    else:
        needed_rec_length = 1
    drive_ids = [int(x["vsphere_key"]) for x in desired_disk_details]
    rec_keys = _get_required_recommendations(rec_result, needed_rec_length, drive_ids)
    task = vi_content.storageResourceManager.ApplyStorageDrsRecommendation_Task(rec_keys)
    # task = vi_content.storageResourceManager.ApplyStorageDrsRecommendation_Task(rec_key[1].key)
    result = VsphereHelpers.wait_task(task)
    return result


def _convert_disk_list_to_dict(disks):
    disk_dict = {}
    for disk in range(len(disks)):
        disk_dict[disks[disk]["label"]] = disks[disk]
    return disk_dict


def _get_required_recommendations(rec_result, needed_rec_length, drive_ids):
    rec_keys = []
    for rec_key in enumerate(rec_result.recommendations):
        if len(rec_keys) < needed_rec_length:
            for action in rec_key[1].action:
                if len(action.relocateSpec.disk) > 0:
                    for rel_disk in action.relocateSpec.disk:
                        if rel_disk.diskId in drive_ids:
                            if rec_key[1].key not in rec_keys:
                                rec_keys.append(rec_key[1].key)
                            drive_ids.remove(rel_disk.diskId)
                        else:
                            continue
                else:
                    if rec_key[1].key not in rec_keys:
                        rec_keys.append(rec_key[1].key)
                    continue
        else:
            return rec_keys
    return rec_keys


def main():
    vm = None

    module = AnsibleModule(
        argument_spec=dict(
            vcenter_hostname=dict(required=True, type='str'),
            vcenter_username=dict(required=True, type='str'),
            vcenter_password=dict(required=True, type='str'),
            guest=dict(required=True, type='str'),
            template_src=dict(required=True, type='str'),
            vm_disk=dict(required=True, type='dict'),
            cluster=dict(required=True, type='str'),
            vm_domain=dict(required=False, type='str'),
            guest_family=dict(required=False, type='str'),
            vm_cpu=dict(required=False, type='int'),
            vm_memory_mb=dict(required=False, type='int'),
            create_template=dict(required=False, choices=BOOLEANS),
            vm_nic=dict(required=False, type='dict', default={}),
            folder_structure=dict(required=False, default=None, type='list'),
            windows_product_id=dict(required=False, default=None, type='str'),
            windows_organization=dict(required=False, default=None, type='str'),
            windows_provisioner_name=dict(required=False, default=None, type='str'),
        ),
        supports_check_mode=False,
    )

    if not HAS_PYVMOMI:
        module.fail_json(msg='pyvmomi module required')

    vcenter_hostname = module.params['vcenter_hostname']
    vcenter_username = module.params['vcenter_username']
    vcenter_password = module.params['vcenter_password']
    guest = module.params['guest']
    template_src = module.params['template_src']
    vm_disk = module.params['vm_disk']
    cluster = module.params['cluster']

    if "vm_cpu" in module.params:
        vm_cpu = module.params['vm_cpu']
    else:
        vm_cpu = None

    if "vm_memory_mb" in module.params:
        vm_memory_mb = module.params['vm_memory_mb']
    else:
        vm_memory_mb = None

    if "create_template" in module.params:
        create_template = module.params['create_template']
    else:
        create_template = False

    if "vm_domain" in module.params:
        vm_domain = module.params['vm_domain']
    else:
        vm_domain = None

    if "guest_family" in module.params:
        os_family = module.params['guest_family']
    else:
        os_family = None

    if "vm_nic" in module.params:
        vm_nic = module.params['vm_nic']
    else:
        vm_nic = None

    if "folder_structure" in module.params:
        folder_structure = module.params['folder_structure']
    else:
        folder_structure = None

    if 'windows_product_id' in module.params:
        product_id = module.params['windows_product_id']
    else:
        product_id = None

    if 'windows_organization' in module.params:
        windows_organization = module.params['windows_organization']
    else:
        windows_organization = None

    if 'windows_provisioner_name' in module.params:
        windows_provisioner_name = module.params['windows_provisioner_name']
    else:
        windows_provisioner_name = None

    # guest_attributes = module.params['guest_attributes']
    si = None
    try:
        si = SmartConnect(
            host=vcenter_hostname,
            user=vcenter_username,
            pwd=vcenter_password
            )

    except Exception as exc:
        try:
            import ssl
            try:
                ssl._create_default_https_context = ssl._create_unverified_context
            except AttributeError:
                pass

            si = SmartConnect(
                host=vcenter_hostname,
                user=vcenter_username,
                pwd=vcenter_password
                )
        except Exception as exc1:
            module.fail_json(msg="Creating unverified context failed. Cannot connect to %s: %s" %(vcenter_hostname, exc1))

    atexit.register(Disconnect, si)

    try:
        content = si.RetrieveContent()
        vm_array = VsphereHelpers.get_vm(si, guest)
        if len(vm_array) > 0:
            if create_template:
                module.exit_json(changed=False)
            else:
                module.fail_json(msg="Found existing VM with name %s" % guest)
        
        changed = False
        try:
            changes = deploy_template(vsphere=si,
                                              vi_content=content,
                                              guest=guest,
                                              template_src=template_src,
                                              cluster_name=cluster,
                                              domain=vm_domain,
                                              vm_cpu=vm_cpu,
                                              vm_memory_mb=vm_memory_mb,
                                              os_family=os_family,
                                              vm_disk=vm_disk,
                                              vm_nic=vm_nic,
                                              windows_product_id=product_id,
                                              windows_org_name=windows_organization,
                                              windows_provision_user=windows_provisioner_name,
                                              is_template=create_template,
                                              folder_structure=folder_structure)
        except Exception as err:
            module.fail_json(msg=err.message)

        if len(changes) > 0:
            changed = True

        module.exit_json(
            changed=changed,
            vcenter=vcenter_hostname,
            changes=changes
        )
    except Exception, err:
        module.fail_json(msg="Could not clone vm: %s. %s" % (guest, err))

# this is magic, see lib/ansible/module_common.py
#<<INCLUDE_ANSIBLE_MODULE_COMMON>>
if __name__ == '__main__':
    main()
