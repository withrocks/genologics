"""Python interface to GenoLogics LIMS via its REST API.

Entities and their descriptors for the LIMS interface.

Per Kraulis, Science for Life Laboratory, Stockholm, Sweden.
Copyright (C) 2012 Per Kraulis
"""

from genologics.constants import nsmap

try:
    from urllib.parse import urlsplit, urlparse, parse_qs, urlunparse
except ImportError:
    from urlparse import urlsplit, urlparse, parse_qs, urlunparse

import datetime
import time
from xml.etree import ElementTree

import logging
import collections

logger = logging.getLogger(__name__)


# An entity can be in different "fetch states". This affects how the object's attributes
# are loaded. The details of what these mean depends on the entity.
# But generally the status is NONE if it hasn't been loaded at all, BASIC if it has been fetched
# in some kind of overview and FETCH_STATE_FULL if it has been fully fetched from the object's
# main REST endpoint (compare e.g. /api/v2/configuration/workflows/
# and /api/v2/configuration/workflows/51)
FETCH_STATE_NONE     = 0
FETCH_STATE_OVERVIEW = 1
FETCH_STATE_DETAILS  = 2
FETCH_STATE_OVERVIEW_OR_DETAILS = FETCH_STATE_OVERVIEW | FETCH_STATE_DETAILS


class BaseDescriptor(object):
    "Abstract base descriptor for an instance attribute."
    def __init__(self, required_fetch_state):
        self.required_fetch_state = required_fetch_state  #TODO: Both defined here and in TagDescriptor

    def __get__(self, instance, cls):
        raise NotImplementedError


class TagDescriptor(BaseDescriptor):
    """Abstract base descriptor for an instance attribute
    represented by an XML element.
    """

    # TODO: we can default required_fetch_state if we want, but I want to be explicit to begin with
    def __init__(self, tag, required_fetch_state):
        self.tag = tag
        self.required_fetch_state = required_fetch_state

    def get_node(self, instance):
        if self.tag:
            return instance.root.find(self.tag)
        else:
            return instance.root

    def refresh(self, instance):
        """Updates the descriptor if required"""
        instance.get(required_fetch_state=self.required_fetch_state)


class StringDescriptor(TagDescriptor):
    """An instance attribute containing a string value
    represented by an XML element.
    """
    def __get__(self, instance, cls):
        instance.get()
        node = self.get_node(instance)
        if node is None:
            return None
        else:
            return node.text

    def __set__(self, instance, value):
        instance.get()
        node = self.get_node(instance)
        if node is None:
            # create the new tag
            node = ElementTree.Element(self.tag)
            instance.root.append(node)
        node.text = str(value)


class StringAttributeDescriptor(TagDescriptor):
    """An instance attribute containing a string value
    represented by an XML attribute.
    """
    def __get__(self, instance, cls):
        # Gets the item from the descriptor, loading the object if required
        self.refresh(instance)
        return self.get_from_node(instance.root)

    def __set__(self, instance, value):
        # Sets the item, loading the object if required (lazy-loading)
        self.refresh(instance)
        self.set_node_to(instance.root, value)

    def get_from_node(self, node):
        return node.attrib[self.tag]

    def set_node_to(self, node, value):
        node.attrib[self.tag] = value


class StringListDescriptor(TagDescriptor):
    """An instance attribute containing a list of strings
    represented by multiple XML elements.
    """
    def __get__(self, instance, cls):
        instance.get()
        result = []
        for node in instance.root.findall(self.tag):
            result.append(node.text)
        return result


class StringDictionaryDescriptor(TagDescriptor):
    """An instance attribute containing a dictionary of string key/values
    represented by a hierarchical XML element.
    """
    def __get__(self, instance, cls):
        instance.get()
        result = dict()
        node = instance.root.find(self.tag)
        if node is not None:
            for node2 in node.getchildren():
                result[node2.tag] = node2.text
        return result


class IntegerDescriptor(StringDescriptor):
    """An instance attribute containing an integer value
    represented by an XMl element.
    """

    def __get__(self, instance, cls):
        text = super(IntegerDescriptor, self).__get__(instance, cls)
        if text is not None:
            return int(text)


class IntegerAttributeDescriptor(TagDescriptor):
    """An instance attribute containing a integer value
    represented by an XML attribute.
    """

    def __get__(self, instance, cls):
        instance.get()
        return self.get_from_node(instance.root)

    def get_from_node(self, node):
        return int(node.attrib[self.tag])


class BooleanDescriptor(StringDescriptor):
    """An instance attribute containing a boolean value
    represented by an XMl element.
    """
    def __get__(self, instance, cls):
        text = super(BooleanDescriptor, self).__get__(instance, cls)
        if text is not None:
            return text.lower() == 'true'

    def __set__(self, instance, value):
        super(BooleanDescriptor, self).__set__(instance, str(value).lower())


class UdfDictionary(object):
    "Dictionary-like container of UDFs, optionally within a UDT."

    def _is_string(self, value):
        try:
            return isinstance(value, basestring)
        except:
            return isinstance(value, str)

    def __init__(self, instance, *args, **kwargs):
        self.instance = instance
        self._udt = kwargs.pop('udt', False)
        self.rootkeys = args
        self._rootnode = None
        self._update_elems()
        self._prepare_lookup()
        self.location = 0

    @property
    def rootnode(self):
        if not self._rootnode:
            self._rootnode = self.instance.root
            for rootkey in self.rootkeys:
                self._rootnode = self._rootnode.find(rootkey)
        return self._rootnode

    def get_udt(self):
        if self._udt == True:
            return None
        else:
            return self._udt

    def set_udt(self, name):
        assert isinstance(name, str)
        if not self._udt:
            raise AttributeError('cannot set name for a UDF dictionary')
        self._udt = name
        elem = self.rootnode.find(nsmap('udf:type'))
        assert elem is not None
        elem.set('name', name)

    udt = property(get_udt, set_udt)

    def _update_elems(self):
        self._elems = []
        if self._udt:
            elem = self.rootnode.find(nsmap('udf:type'))
            if elem is not None:
                self._udt = elem.attrib['name']
                self._elems = elem.findall(nsmap('udf:field'))
        else:
            tag = nsmap('udf:field')
            for elem in self.rootnode.getchildren():
                if elem.tag == tag:
                    self._elems.append(elem)

    def _prepare_lookup(self):
        self._lookup = dict()
        for elem in self._elems:
            type = elem.attrib['type'].lower()
            value = elem.text
            if not value:
                value = None
            elif type == 'numeric':
                try:
                    value = int(value)
                except ValueError:
                    value = float(value)
            elif type == 'boolean':
                value = value == 'true'
            elif type == 'date':
                value = datetime.date(*time.strptime(value, "%Y-%m-%d")[:3])
            self._lookup[elem.attrib['name']] = value

    def __contains__(self, key):
        try:
            self._lookup[key]
        except KeyError:
            return False
        return True

    def __getitem__(self, key):
        return self._lookup[key]

    def __setitem__(self, key, value):
        self._lookup[key] = value
        for node in self._elems:
            if node.attrib['name'] != key: continue
            vtype = node.attrib['type'].lower()

            if value is None:
                pass
            elif vtype == 'string':
                if not self._is_string(value):
                    raise TypeError('String UDF requires str or unicode value')
            elif vtype == 'str':
                if not self._is_string(value):
                    raise TypeError('String UDF requires str or unicode value')
            elif vtype == 'text':
                if not self._is_string(value):
                    raise TypeError('Text UDF requires str or unicode value')
            elif vtype == 'numeric':
                if not isinstance(value, (int, float)):
                    raise TypeError('Numeric UDF requires int or float value')
                value = str(value)
            elif vtype == 'boolean':
                if not isinstance(value, bool):
                    raise TypeError('Boolean UDF requires bool value')
                value = value and 'true' or 'false'
            elif vtype == 'date':
                if not isinstance(value, datetime.date):  # Too restrictive?
                    raise TypeError('Date UDF requires datetime.date value')
                value = str(value)
            elif vtype == 'uri':
                if not self._is_string(value):
                    raise TypeError('URI UDF requires str or punycode (unicode) value')
                value = str(value)
            else:
                raise NotImplemented("UDF type '%s'" % vtype)
            if not isinstance(value, str):
                if not self._is_string(value):
                    value = str(value).encode('UTF-8')
            node.text = value
            break
        else:  # Create new entry; heuristics for type
            if self._is_string(value):
                vtype = '\n' in value and 'Text' or 'String'
            elif isinstance(value, bool):
                vtype = 'Boolean'
                value = value and 'true' or 'false'
            elif isinstance(value, (int, float)):
                vtype = 'Numeric'
                value = str(value)
            elif isinstance(value, datetime.date):
                vtype = 'Date'
                value = str(value)
            else:
                raise NotImplementedError("Cannot handle value of type '%s'"
                                          " for UDF" % type(value))
            if self._udt:
                root = self.rootnode.find(nsmap('udf:type'))
            else:
                root = self.rootnode
            elem = ElementTree.SubElement(root,
                                          nsmap('udf:field'),
                                          type=vtype,
                                          name=key)
            if not isinstance(value, str):
                if not self._is_string(value):
                    value = str(value).encode('UTF-8')

            elem.text = value

            #update the internal elements and lookup with new values
            self._update_elems()
            self._prepare_lookup()

    def __delitem__(self, key):
        del self._lookup[key]
        for node in self._elems:
            if node.attrib['name'] == key:
                self.rootnode.remove(node)
                break

    def items(self):
        return list(self._lookup.items())

    def clear(self):
        for elem in self._elems:
            self.rootnode.remove(elem)
        self._update_elems()

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        try:
            ret = list(self._lookup.keys())[self.location]
        except IndexError:
            raise StopIteration()
        self.location = self.location + 1
        return ret

    def get(self, key, default=None):
        return self._lookup.get(key, default)


class UdfDictionaryDescriptor(BaseDescriptor):
    """An instance attribute containing a dictionary of UDF values
    represented by multiple XML elements.
    """
    _UDT = False

    def __init__(self, required_fetch_status, *args):
        super(BaseDescriptor, self).__init__()
        self.rootkeys = args
        self.required_fetch_status = required_fetch_status

    def __get__(self, instance, cls):
        instance.get()
        self.value = UdfDictionary(instance, *self.rootkeys, udt=self._UDT)
        return self.value

    def __set__(self, instance, dict_value):
        instance.get()
        udf_dict = UdfDictionary(instance, *self.rootkeys, udt=self._UDT)
        udf_dict.clear()
        for k in dict_value:
            udf_dict[k] = dict_value[k]


class UdtDictionaryDescriptor(UdfDictionaryDescriptor):
    """An instance attribute containing a dictionary of UDF values
    in a UDT represented by multiple XML elements.
    """

    _UDT = True


class PlacementDictionaryDescriptor(TagDescriptor):
    """An instance attribute containing a dictionary of locations
    keys and artifact values represented by multiple XML elements.
    """
    def __get__(self, instance, cls):
        from genologics.entities import Artifact
        instance.get()
        self.value = dict()
        for node in instance.root.findall(self.tag):
            key = node.find('value').text
            self.value[key] = Artifact(instance.lims, uri=node.attrib['uri'])
        return self.value


class ExternalidListDescriptor(BaseDescriptor):
    """An instance attribute yielding a list of tuples (id, uri) for
    external identifiers represented by multiple XML elements.
    """

    def __get__(self, instance, cls):
        instance.get()
        result = []
        for node in instance.root.findall(nsmap('ri:externalid')):
            result.append((node.attrib.get('id'), node.attrib.get('uri')))
        return result


class EntityDescriptor(TagDescriptor):
    "An instance attribute referencing another entity instance."

    def __init__(self, tag, klass, required_fetch_state):
        super(EntityDescriptor, self).__init__(tag, required_fetch_state)
        self.klass = klass

    def __get__(self, instance, cls):
        instance.get()
        node = instance.root.find(self.tag)
        if node is None:
            return None
        else:
            return self.klass(instance.lims, uri=node.attrib['uri'])

    def __set__(self, instance, value):
        instance.get()
        node = self.get_node(instance)
        if node is None:
            # create the new tag
            node = ElementTree.Element(self.tag)
            instance.root.append(node)
        node.attrib['uri'] = value.uri


class EntityListDescriptor(EntityDescriptor):
    """An instance attribute yielding a list of entity instances
    represented by multiple XML elements.
    """

    def __get__(self, instance, cls):
        instance.get()
        result = []
        for node in instance.root.findall(self.tag):
            result.append(self.klass(instance.lims, uri=node.attrib['uri']))

        return result


class NestedAttributeListDescriptor(StringAttributeDescriptor):
    """An instance yielding a list of dictionnaries of attributes
       for a nested xml list of XML elements"""
    def __init__(self, tag, rootkeys, required_fetch_state):
        super(StringAttributeDescriptor, self).__init__(tag, required_fetch_state)
        self.tag = tag
        self.rootkeys = rootkeys

    def __get__(self, instance, cls):
        instance.get()
        result = []
        rootnode = instance.root
        for rootkey in self.rootkeys:
            rootnode = rootnode.find(rootkey)
        for node in rootnode.findall(self.tag):
            result.append(node.attrib)
        return result


class NestedStringListDescriptor(StringListDescriptor):
    """An instance yielding a list of strings
        for a nested list of xml elements"""

    def __init__(self, tag, rootkeys, required_fetch_state):
        super(StringListDescriptor, self).__init__(tag, required_fetch_state)
        self.tag = tag
        self.rootkeys = rootkeys

    def __get__(self, instance, cls):
        instance.get()
        result = []
        rootnode = instance.root
        for rootkey in self.rootkeys:
            rootnode = rootnode.find(rootkey)
        for node in rootnode.findall(self.tag):
            result.append(node.text)
        return result


class NestedEntityListDescriptor(EntityListDescriptor):
    """same as EntityListDescriptor, but works on nested elements"""

    def __init__(self, tag, klass, rootkeys, required_fetch_state):
        super(EntityListDescriptor, self).__init__(tag, klass, required_fetch_state)
        self.klass = klass
        self.tag = tag
        self.rootkeys = rootkeys if isinstance(rootkeys, list) else [rootkeys]
        assert self.required_fetch_state > 0

    def __get__(self, instance, cls):
        instance.get(required_fetch_state=self.required_fetch_state)
        result = []
        rootnode = instance.root
        for rootkey in self.rootkeys:
            rootnode = rootnode.find(rootkey)
        for node in rootnode.findall(self.tag):
            child_instance = self.create_from_overview_node(instance, node)
            result.append(child_instance)
        return result

    # TODO: In superclass or somewhere else?!
    def create_from_overview_node(self, parent_obj, node):
        # The fetch state we're in:
        fetch_state = FETCH_STATE_OVERVIEW
        instance = self.klass(parent_obj.lims, uri=node.attrib['uri'], fetch_state=fetch_state)
        instance.root = node

        # Initialize based on this node, here we turn the descriptors around and use them to query for
        # the data in the XML, but when accessing the descriptors directly, the descriptor fetches from the XML.
        # So now the descriptors are both used for metadata and as lazy properties
        for attr, descriptor in self.klass.__dict__.items():
            if attr.startswith("_"):
                continue
            # TODO: Allow the descriptor to have something else than required_fetch_state, i.e. try catch
            can_update = fetch_state & descriptor.required_fetch_state != 0
            if can_update:
                # Use the parsing built into the descriptor to get the value. NOTE: This is really backwards!
                val = descriptor.get_from_node(node)
                descriptor.__set__(instance, val)
        return instance


class DimensionDescriptor(TagDescriptor):
    """An instance attribute containing a dictionary specifying
    the properties of a dimension of a container type.
    """
    def __get__(self, instance, cls):
        instance.get()
        node = instance.root.find(self.tag)
        return dict(is_alpha=node.find('is-alpha').text.lower() == 'true',
                    offset=int(node.find('offset').text),
                    size=int(node.find('size').text))


class LocationDescriptor(TagDescriptor):
    """An instance attribute containing a tuple (container, value)
    specifying the location of an analyte in a container.
    """
    def __get__(self, instance, cls):
        from genologics.entities import Container
        instance.get()
        node = instance.root.find(self.tag)
        uri = node.find('container').attrib['uri']
        return Container(instance.lims, uri=uri), node.find('value').text


class ReagentLabelList(BaseDescriptor):
    """An instance attribute yielding a list of reagent labels"""
    def __get__(self, instance, cls):
        instance.get()
        self.value = []
        for node in instance.root.findall('reagent-label'):
            try:
                self.value.append(node.attrib['name'])
            except:
                pass
        return self.value


class InputOutputMapList(BaseDescriptor):
    """An instance attribute yielding a list of tuples (input, output)
    where each item is a dictionary, representing the input/output
    maps of a Process instance.
    """

    def __init__(self, required_fetch_state, *args):
        super(BaseDescriptor, self).__init__()
        self.rootkeys = args
        self.required_fetch_state = required_fetch_state

    def __get__(self, instance, cls):
        instance.get()
        self.value = []
        rootnode = instance.root
        for rootkey in self.rootkeys:
            rootnode = rootnode.find(rootkey)
        for node in rootnode.findall('input-output-map'):
            input = self.get_dict(instance.lims, node.find('input'))
            output = self.get_dict(instance.lims, node.find('output'))
            self.value.append((input, output))
        return self.value

    def get_dict(self, lims, node):
        from genologics.entities import Artifact, Process
        if node is None: return None
        result = dict()
        for key in ['limsid', 'output-type', 'output-generation-type']:
            try:
                result[key] = node.attrib[key]
            except KeyError:
                pass
            for uri in ['uri', 'post-process-uri']:
                try:
                    result[uri] = Artifact(lims, uri=node.attrib[uri])
                except KeyError:
                    pass
        node = node.find('parent-process')
        if node is not None:
            result['parent-process'] = Process(lims, node.attrib['uri'])
        return result
