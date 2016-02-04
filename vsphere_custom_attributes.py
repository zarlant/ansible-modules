#!/usr/bin/python

import atexit

HAS_PYVMOMI = False
try:
    from pyVmomi import vim
    from pyVmomi import vmodl
    from pyVim.connect import SmartConnect, Disconnect
    HAS_PYVMOMI = True

except ImportError as e:
    raise Exception(str(e))


def collect_properties(service_instance, view_ref, obj_type, path_set=None,
                       include_mors=False):
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


def get_vm(service_instance, vm_name):
    view = get_container_view(service_instance,
                                   obj_type=[vim.VirtualMachine])

    vm_data = collect_properties(service_instance, view_ref=view,
                                      obj_type=vim.VirtualMachine,
                                      path_set=["name"],
                                      include_mors=True)

    vms = [x['obj'] for x in vm_data if x["name"] == vm_name]

    return vms


def get_obj(content, vimtype, name):
    obj = None
    container = content.viewManager.CreateContainerView(
        content.rootFolder, vimtype, True)
    for c in container.view:
        if c.name == name:
            obj = c
            break
    return obj

def main():

    vm = None

    module = AnsibleModule(
        argument_spec=dict(
            vcenter_hostname=dict(required=True, type='str'),
            vcenter_username=dict(required=True, type='str'),
            vcenter_password=dict(required=True, type='str'),
            guest_attributes=dict(required=True, type='dict'),
            guest=dict(required=True, type='str'),
        ),
        supports_check_mode=False,
    )

    if not HAS_PYVMOMI:
        module.fail_json(msg='pyvmomi module required')

    vcenter_hostname = module.params['vcenter_hostname']
    vcenter_username = module.params['vcenter_username']
    vcenter_password = module.params['vcenter_password']
    guest_attributes = module.params['guest_attributes']
    guest = module.params['guest']
    si = None
    try:
        si = SmartConnect(
            host=vcenter_hostname,
            user=vcenter_username,
            pwd=vcenter_password
            )
    except Exception, err:
        module.fail_json(msg="Cannot connect to %s: %s" %(vcenter_hostname, err))

    # disconnect this thing
    atexit.register(Disconnect, si)

    try:
        content = si.RetrieveContent()
        vm = get_vm(si, guest)[0]
        changes = []
        failed_keys = []
        available_attributes = [x.name for x in vm.availableField]

        for key in guest_attributes:
            if key in available_attributes:
                try:
                    vm.setCustomValue(key=key, value=guest_attributes[key])
                    changes.append("%s:%s" % (key, guest_attributes[key]))
                except Exception as e:
                    failed_keys.append("%s:%s" % (key, guest_attributes[key]))

        module.exit_json(
            changed=True,
            vcenter=vcenter_hostname,
            changes=changes,
            failed_keys=failed_keys
        )
    except Exception, err:
        module.fail_json(msg="Could not set attributes on vm: %s. %s" % (guest, err))


# this is magic, see lib/ansible/module_common.py
#<<INCLUDE_ANSIBLE_MODULE_COMMON>>
if __name__ == '__main__':
    main()
