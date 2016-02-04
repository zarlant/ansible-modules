#!/usr/bin/python
try:
    import json
except ImportError:
    import simplejson as json

HAS_PYSPHERE = False
HAS_NET_HELPER = False
try:
    from pysphere import VIServer, VIProperty, MORTypes
    from pysphere.resources import VimService_services as VI
    from pysphere.vi_task import VITask
    from pysphere import VIException, VIApiException  #, FaultType
    from tempfile import NamedTemporaryFile

    import os
    import time
    HAS_PYSPHERE = True

except ImportError as e:
    raise Exception(str(e))

DOCUMENTATION = '''
---
module: vsphere_folder_relocate
short_description: Creates folder and moves VM(s) to that folder VMware vSphere.
description:
     - Moves VM(s) to folder specified in the form of an array. Creates folder if needed and the base folder exists. This module has a dependency on pysphere >= 1.7
version_added: "1.9"
options:
  vcenter_hostname:
    description:
      - The hostname of the vcenter server the module will connect to, to create the guest.
    required: true
    default: null
    aliases: []
  guest_list:
    description:
      - Array of servers to move into folder.
    required: true
  vcenter_username:
    description:
      - Username to connect to vcenter as.
    required: true
    default: null
  vcenter_password:
    description:
      - Password of the user to connect to vcenter as.
    required: true
    default: null
  folder_structure:
    description:
      - Array of strings representing the folder structure desired.
    required: true

notes:
  - This module should run from a system that can access vSphere directly.
    Either by using local_action, or using delegate_to.
author: Zacharias Thompson <zarlant@gmail.com>
requirements:
  - "python >= 2.6"
  - pysphere
'''


EXAMPLES = '''
# Find or Create a folder in vCenter. Move specified VMs into new or existing folder.
# Returns changed = False when the VM(s) already exist in the specified folder
# Returns changed = True when it moves VM(s)

- vsphere_folder_relocate:
    vcenter_hostname: vcenter.mydomain.local
    username: myuser
    password: mypass
    guest: newvm001
    state: powered_on
    vm_extra_config:
      vcpu.hotadd: yes
      mem.hotadd:  yes
      notes: This is a test VM
  - name: Move VM to Correct Folder
    vsphere_folder_relocate:
    vcenter_hostname: "vcenter"
    vcenter_username: "vcenter_account"
    vcenter_password: "vcenter_pass"
    guest_list:
       - "my_vm_to_move"
     folder_structure:
       - "some_base_folder"
       - "next_folder_level"
       - "environment_folder"
'''

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

def create_folder(viserver, root_folder, new_name):
    request = VI.CreateFolderRequestMsg()
    _this = request.new__this(root_folder)
    _this.set_attribute_type(root_folder.get_attribute_type())
    request.set_element__this(_this)
    request.set_element_name(new_name)
    viserver._proxy.CreateFolder(request)

def get_folder_objects(viserver):
    folders = viserver._retrieve_properties_traversal(
                                     property_names=['name', 'childEntity', 'parent'],
                                     obj_type=MORTypes.Folder)

    folder_objects = [{"folder": x.Obj,
              "child": [y.Val.ManagedObjectReference for y in x.PropSet if y.Name == "childEntity"][0],
              "name": [y.Val for y in x.PropSet if y.Name == "name"][0],
              "parent": [y.Val for y in x.PropSet if y.Name == "parent"]
             } for x in folders]

    return folder_objects


def get_datacenter_folder(viserver, base_datacenter):
    dcs = viserver._retrieve_properties_traversal(
        property_names=['name', 'vmFolder'],
        obj_type=MORTypes.Datacenter
    )

    dc_objects = [{"dc": x.Obj,
          "folder": [y.Val for y in x.PropSet if y.Name == "vmFolder"][0],
          "name": [y.Val for y in x.PropSet if y.Name == "name"][0]
         } for x in dcs]

    base = [x for x in dc_objects if x["name"] == base_datacenter]
    if base is not None:
        return base[0]
    else:
        return None


def find_folder(viserver, folder_structure, folder_objects, dc_folder):
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
    [root_tree.leaves.remove(y) for y in [x for x in root_tree.leaves if dc_folder["folder"] not in x.meta_data["parent"]]]
    deep_leaves = pytree.get_deepest_leaves(root_tree)

    if not isinstance(deep_leaves, list):
        if isinstance(deep_leaves, pytree):
            if deep_leaves.name == "root":
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
            root_folder, folder_children = find_folder(viserver, folder_structure[:-1], folder_objects, dc_folder)
            create_folder(viserver, root_folder, folder_structure[-1])
            return find_folder(viserver, folder_structure, get_folder_objects(viserver), dc_folder)
        else:
            raise Exception("Could not find any matching folder for structure: %s." % json.dumps(folder_structure))

    return folder_mor, folder_children


def main():
    vm = None

    module = AnsibleModule(
        argument_spec=dict(
            vcenter_hostname=dict(required=True, type='str'),
            vcenter_username=dict(required=True, type='str'),
            vcenter_password=dict(required=True, type='str'),
            datacenter_name=dict(required=True, type='str'),
            folder_structure=dict(required=True, type='list'),
            guest_list=dict(required=True, type='list'),
        ),
        supports_check_mode=False,
    )

    if not HAS_PYSPHERE:
        module.fail_json(msg='pysphere module required')

    vcenter_hostname = module.params['vcenter_hostname']
    vcenter_username = module.params['vcenter_username']
    vcenter_password = module.params['vcenter_password']
    guest_list = module.params['guest_list']
    folder_structure = module.params['folder_structure']
    base_datacenter = module.params['datacenter_name']

    # CONNECT TO THE SERVER
    viserver = VIServer()
    try:
        viserver.connect(vcenter_hostname, vcenter_username, vcenter_password)
    except VIApiException, err:
        module.fail_json(msg="Cannot connect to %s: %s" %
                         (vcenter_hostname, err))

    vm_mors = []
    found_vms = []
    for guest in guest_list:
        # Check if the VM exists before continuing
        try:
            vm = viserver.get_vm_by_name(guest)
            vm_mors.append(vm._mor)
            found_vms.append(guest)
        except Exception:
            pass
    changed = False
    if len(vm_mors) > 0:
        folder_mor = None
        folder_children = []
        try:
            folder_objects = get_folder_objects(viserver)
            dc_folder = get_datacenter_folder(viserver, base_datacenter)
            folder_mor, folder_children = find_folder(viserver, folder_structure, folder_objects, dc_folder)

            temp_mors = []
            for vm in vm_mors:
                if vm not in folder_children:
                    temp_mors.append(vm)
            vm_mors = temp_mors
            if len(vm_mors) == 0:
                viserver.disconnect()
                module.exit_json(changed=False)
        except Exception as e:
            viserver.disconnect()
            module.fail_json(msg=str(e))

        try:
            req = VI.MoveIntoFolder_TaskRequestMsg()
            req.set_element__this(folder_mor)
            req.set_element_list(vm_mors)
            task = VITask(viserver._proxy.MoveIntoFolder_Task(req).Returnval, viserver)
            task.wait_for_state([task.STATE_SUCCESS, task.STATE_ERROR])

            if task.get_state() == task.STATE_ERROR:
                viserver.disconnect()
                module.fail_json(msg="Error moving vm: %s to folder %s. Error: %s" %
                                 (found_vms, json.dumps(folder_structure), task.get_error_message()))
            else:
                changed = True
        except Exception as e:
            viserver.disconnect()
            module.fail_json(msg="Error Requesting VM Move: %s for VM: %s" % (found_vms, json.dumps(folder_structure), str(e)))

    viserver.disconnect()
    module.exit_json(
        changed=changed,
        changes=found_vms)


# this is magic, see lib/ansible/module_common.py
#<<INCLUDE_ANSIBLE_MODULE_COMMON>>
if __name__ == '__main__':
    main()
