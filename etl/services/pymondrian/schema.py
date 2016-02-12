# -*- coding: utf-8 -*-

'''
The MIT License (MIT)

Copyright (c) 2014 Juan Gabito

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''


from .core.attribute import Attribute as Property
from .core.element import SchemaElement


class Schema(SchemaElement):
    def __init__(self, name, description=None, measures_caption=None,
                 default_role=None, metamodel_version='4.0'):
        SchemaElement.__init__(self, name)
        self._description = Property('description', description)
        self._measures_caption = Property('measuresCaption', measures_caption)
        self._default_role = Property('defaultRole', default_role)
        self._metamodel_version = Property('metamodelVersion', metamodel_version)
        self._cubes = []
        self._physical_schema = []

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description.value = description

    @property
    def measures_caption(self):
        return self._measures_caption

    @measures_caption.setter
    def measures_caption(self, measures_caption):
        self._measures_caption.value = measures_caption

    @property
    def default_role(self):
        return self._default_role

    @default_role.setter
    def default_role(self, default_role):
        self._default_role.value = default_role

    def add_physical_schema(self, physical_schema):
        if self._physical_schema:
            raise Exception('''Physical schema already exists in schema''')
        self._physical_schema = physical_schema

    def add_cube(self, cube):
        for cub in self._cubes:
            if cub.name == cube.name:
                raise Exception('''Cube "{0}" already exists in schema
                                "{1}"'''.format(cube.name, self.name))
        self._cubes.append(cube)

    def remove_cube(self, cube):
        super(Schema, self).remove_child(cube, self._cubes, type(Cube))

    def get_cube(self, cube_name):
        for cub in self._cubes:
            if cub.name == cube_name:
                return cub

        return None

    @property
    def cubes(self):
        return self._cubes


class PhysicalSchema(SchemaElement):

    def __init__(self):
        super(PhysicalSchema, self).__init__(name=None)
        self._tables = []

    def add_table(self, table):
        for t in self._tables:
            if t.name == table.name:
                raise Exception('''Table "{0}" already exists in Physical Schema
                                "{1}"'''.format(table.name, self.name))
        self._tables.append(table)

    def add_tables(self, tables):
        for table in tables:
            self.add_table(table)


class Cube(SchemaElement):
    def __init__(self, name, description=None, caption=None, cache=True,
                 enabled=True, visible=True, default_measure=None):
        SchemaElement.__init__(self, name)
        self._description = Property('description', description)
        self._caption = Property('caption', caption)
        self._cache = Property('cache', cache)
        self._enabled = Property('enabled', enabled)
        self._visible = Property('visible', visible)
        self._default_measure = Property('defaultMeasure', default_measure)
        self._afact = None

        self.dimensions_tag = Dimensions()
        self.measure_groups = MeasureGroups()
        self._dimensions = [self.dimensions_tag]
        self._measures = [self.measure_groups]

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description.value = description

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cache.value = cache

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, enabled):
        self._enabled.value = enabled

    @property
    def visible(self):
        return self._visible

    @visible.setter
    def visible(self, visible):
        self._visible.value = visible

    @property
    def fact(self):
        return self._afact

    @fact.setter
    def fact(self, fact):
        self._afact = fact

    @property
    def default_measure(self):
        return self._default_measure

    @default_measure.setter
    def default_measure(self, default_measure):
        self._default_measure = default_measure

    def add_dimension(self, dimension):
        for dim in self.dimensions_tag._dimensions:
            if dim.name == dimension.name:
                raise Exception('''Dimension "{0}" already exists in cube
                                "{1}"'''.format(dimension.name, self.name))
        self.dimensions_tag._dimensions.append(dimension)

    def remove_dimension(self, dimension):
        self.remove_child(
            dimension, self.dimensions_tag._dimensions, type(Dimension))

    def get_dimension(self, dimension_name):
        for dim in self.dimensions_tag._dimensions:
            if dim.name == dimension_name:
                return dim
        return None

    def add_measures_groups(self, measure):
        for mea in self._measures:
            if mea.name == measure.name:
                raise Exception('''Measure "{0}" already exists in cube
                                "{1}"'''.format(measure.name, self.name))
        self._measures.append(measure)

    def remove_measure(self, measure):
        super(Cube, self).remove_child(measure, self._measures, type(Measure))

    def get_measure(self, measure_name):
        for mea in self._measures:
            if mea.name == measure_name:
                return mea
        return None

    @property
    def dimensions(self):
        return self.dimensions_tag._dimensions

    @property
    def measures(self):
        return self._measures


class Dimensions(SchemaElement):
    """
    Тег Dimensions, которые содержит все размерности
    """
    def __init__(self):
        super(Dimensions, self).__init__(name=None)
        self._dimensions = []


class MeasureGroups(SchemaElement):
    """
    Тег MeasureGroups, которые содержит все меры
    """
    def __init__(self):
        super(MeasureGroups, self).__init__(name=None)
        self._measure_groups = []

    def add_measure_group(self, measure_group):
        for m in self._measure_groups:
            if m.name == measure_group.name:
                raise Exception('''Measure "{0}" already exists in Measures
                                "{1}"'''.format(measure_group.name, self.name))
        self._measure_groups.append(measure_group)


class MeasureGroup(SchemaElement):

    def __init__(self, name, table):
        super(MeasureGroup, self).__init__(name=name)
        self._table = Property('table', table)
        self.measures_tag = Measures()
        self.dimension_links = DimensionLinks()
        self._measures = [self.measures_tag]
        self._dimensions_link = [self.dimension_links]

    def add_measures(self, measure):
        self.measures_tag.add_measures(measure)

    def add_dimension_link(self, dim_link):
        self.dimension_links.add_dimension_link(dim_link)


class Measures(SchemaElement):
    def __init__(self):
        super(Measures, self).__init__(name=None)
        self._measures = []

    def add_measures(self, measure):
        for m in self._measures:
            if m.name == measure.name:
                raise Exception('''Measure "{0}" already exists in Measures
                                "{1}"'''.format(measure.name, self.name))
        self._measures.append(measure)


class DimensionLinks(SchemaElement):
    """
    Тег MeasureGroups, которые содержит все меры
    """
    def __init__(self):
        super(DimensionLinks, self).__init__(name=None)
        self._dimension_links = []

    def add_dimension_link(self, dimension_link):
        for dim_link in self._dimension_links:
            if dim_link._dimension == dimension_link._dimension:
                raise Exception('''Dimension Link "{0}" already exists in Measures
                '''.format(dimension_link._dimension))
        self._dimension_links.append(dimension_link)


class DimensionLink(SchemaElement):

    def __init__(self, dimension):
        super(DimensionLink, self).__init__(name=None)
        self._dimension = Property('dimension', dimension)


class NoLink(DimensionLink):
    pass


class ForeignKeyLink(DimensionLink):

    def __init__(self, dimension, foreign_key_column):
        super(ForeignKeyLink, self).__init__(dimension=dimension)
        self._foreign_key_column = Property(
            'foreignKeyColumn', foreign_key_column)


class ReferenceLink(DimensionLink):

    def __init__(self, dimension, via_dimension, via_attribute, attribute=None):
        super(ReferenceLink, self).__init__(dimension=dimension)
        self._via_dimension = Property('viaDimension', via_dimension)
        self._via_attribute = Property('viaAttribute', via_attribute)
        self._attribute = Property('attribute', attribute)


class Table(SchemaElement):
    def __init__(self, name, schema=None, alias=None):
        SchemaElement.__init__(self, name)
        self._schema = Property('schema', schema)
        self._alias = Property('alias', alias)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema.value = schema

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, alias):
        self._alias.value = alias


class Hierarchies(SchemaElement):
    """
    Иерархии. На данные момент предполагается,
    что у размерости только один подобный тег
    """
    def __init__(self):
        super(Hierarchies, self).__init__(name=None)
        self._hierarchies = []

    def add_hierarchy(self, hierarchy):
        for hry in self._hierarchies:
            if hry.name == hierarchy.name:
                raise Exception('''Hierarchy "{0}" already exists in dimension
                                "{1}"'''.format(hierarchy.name, self.name))
        self._hierarchies.append(hierarchy)

    def add_level_to_hierarchy(self, hierarchy, level,
                               level_position=None):
        if type(hierarchy) is int and hierarchy < len(self._hierarchies):
            self._hierarchies[hierarchy].add_level(level, level_position)
        if type(hierarchy) is str or type(hierarchy) is Hierarchy:
            hierachy_name = str(hierarchy)
            for hry in self._hierarchies:
                if hry.name == hierachy_name:
                    hry.add_level(level, level_position)

    def get_hierarchy(self, hierarchy):
        if type(hierarchy) is int:
            if 0 <= hierarchy < len(self._hierarchies):
                return self._hierarchies[hierarchy]
        elif type(hierarchy) is str:
            hierarchy_index = -1
            for hry in self._hierarchies:
                hierarchy_index += 1
                if hry.name == hierarchy:
                    return self._hierarchies[hierarchy_index]
        elif type(hierarchy) is Hierarchy:
            hierarchy_index = self._hierarchies.index(hierarchy)
            return self._hierarchies[hierarchy_index]

    def remove_hierarchy(self, hierarchy):
        super(Hierarchies, self).remove_child(
            hierarchy, self._hierarchies, type(Hierarchy))


class Hierarchy(SchemaElement):
    def __init__(self, name, visible='true', has_all='true', all_member_name=None,
                 all_member_caption=None, all_level_name=None,
                 primary_key=None, primary_key_table=None,
                 default_member=None, member_reader_class=None, caption=None,
                 description=None, unique_key_level_name=None):
        super(Hierarchy, self).__init__(name)
        self._visible = Property('visible', visible)
        self._has_all = Property('hasAll', has_all)
        self._all_member_name = Property('allMemberName', all_member_name)
        self._all_member_caption = Property('allMemberCaption',
                                             all_member_caption)
        self._all_level_name = Property('allLevelName', all_level_name)
        self._primary_key = Property('primaryKey', primary_key)
        self._primary_key_table = Property('primaryKeyTable',
                                            primary_key_table)
        self._default_member = Property('defaultMember', default_member)
        self._member_reader_class = Property('memberReaderClass',
                                              member_reader_class)
        self._caption = Property('caption', caption)
        self._description = Property('description', description)
        self._unique_key_level_name = Property('uniqueKeyLevelName',
                                                unique_key_level_name)
        # self._atable = Table(name.lower())
        self._levels = []

    @property
    def visible(self):
        return self._visible

    @visible.setter
    def visible(self, visible):
        self._visible.value = visible

    @property
    def has_all(self):
        return self._has_all

    @has_all.setter
    def has_all(self, has_all):
        self._has_all.value = has_all

    @property
    def all_member_name(self):
        return self._all_member_name

    @all_member_name.setter
    def all_member_name(self, all_member_name):
        self._all_member_name.value = all_member_name

    @property
    def all_member_caption(self):
        return self._all_member_caption

    @all_member_caption.setter
    def all_member_caption(self, all_member_caption):
        self._all_member_caption.value = all_member_caption

    @property
    def all_level_name(self):
        return self._all_level_name

    @all_level_name.setter
    def all_level_name(self, all_level_name):
        self._all_level_name.value = all_level_name

    @property
    def primary_key(self):
        return self._primay_key

    @primary_key.setter
    def primary_key(self, primary_key):
        self._primary_key.value = primary_key

    @property
    def primary_key_table(self):
        return self._primary_key_table

    @primary_key_table.setter
    def primary_key_table(self, primary_key_table):
        self._primary_key_table.value = primary_key_table

    @property
    def default_member(self):
        return self._default_member

    @default_member.setter
    def default_member(self, default_member):
        self._default_member.value = default_member

    @property
    def member_reader_class(self):
        return self._member_reader_class

    @member_reader_class.setter
    def member_reader_class(self, member_reader_class):
        self._member_reader_class.value = member_reader_class

    @property
    def caption(self):
        return self._caption

    @caption.setter
    def caption(self, caption):
        self._caption.value = caption

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description.value = description

    @property
    def unique_key_level_name(self):
        return self._unique_key_level_name

    @unique_key_level_name.setter
    def unique_key_level_name(self, unique_key_level_name):
        self._unique_key_level_name.value = unique_key_level_name

    @property
    def table(self):
        return self._atable

    @table.setter
    def tale(self, table):
        self._atable = table

    @property
    def levels(self):
        return self._levels

    def add_level(self, level, level_position=None):
        for lvl in self._levels:
            if lvl._attribute == level._attribute:
                raise Exception('''Level "{0}" already exists in hierarchy
                                "{1}"'''.format(level._attribute, self.name))

        if level_position is None:
            self._levels.append(level)
        else:
            self._levels.insert(level_position, level)

    def remove_level(self, level):
        super(Hierarchy, self).remove_child(level, self._levels, type(Level))

    def get_level(self, level_name):
        for lvl in self._levels:
            if lvl.name == level_name:
                return lvl
        return None

    def add_levels(self, levels):
        for level in levels:
            self.add_level(level)


class CubeDimension(SchemaElement):
    def __init__(self, name, caption=None, visible=True, description=None,
                 foreign_key=None, high_cardinality=False):
        super(CubeDimension, self).__init__(name)
        self._caption = Property('caption', caption)
        self._visible = Property('visible', visible)
        self._description = Property('description', description)
        self._foreign_key = Property('foreignKey', foreign_key)
        self._high_cardinality = Property('highCardinality', high_cardinality)

    @property
    def caption(self):
        return self._caption

    @caption.setter
    def caption(self, caption):
        self._caption.value = caption

    @property
    def visible(self):
        return self.visible

    @visible.setter
    def visible(self, visible):
        self._visible.value = visible

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description.value = description

    @property
    def foreign_key(self):
        return self._foreign_key

    @foreign_key.setter
    def foreign_key(self, foreign_key):
        self._foreign_key.value = foreign_key

    @property
    def high_cardinality(self):
        return self._high_cardinality

    @high_cardinality.setter
    def high_cardinality(self, high_cardinality):
        self._high_cardinality.value = high_cardinality

    # def remove_hierarchy(self, hierarchy):
    #     super(CubeDimension, self).remove_child(
    #         hierarchy, self._hierarchies, type(Hierarchy))


class Dimension(CubeDimension):
    def __init__(self, name, table=None, key=None, type="OTHER", caption=None,
                 description=None, usage_prefix=None, visible=True,
                 foreign_key=None, hight_cardinality=False):
        super(Dimension, self).__init__(
            name, caption, visible, description, foreign_key, hight_cardinality)
        self._table = Property('table', table)
        self._key = Property('key', key)
        self._type = Property('type', type)
        self._usage_prefix = Property('usagePregix', usage_prefix)
        self._hierarchies_set = Hierarchies()
        self._attributes_set = Attributes()


    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type.value = type

    @property
    def usage_prefix(self):
        return self._usage_prefix

    @usage_prefix.setter
    def usage_prefix(self, usage_prefix):
        self._usage_prefix.value = usage_prefix

    def add_attribute(self, attribute):
        for attr in self._attributes_set._attributes:
            if attr.name == attribute.name:
                raise Exception('''Attribute "{0}" already exists in dimension
                                "{1}"'''.format(attribute.name, self.name))
        self._attributes_set._attributes.append(attribute)

    def add_attributes(self, attributes):
        for attr in attributes:
            self.add_attribute(attr)

    def add_hierarchies(self, hierarchies):
        for hierarchy in hierarchies:
            self._hierarchies_set.add_hierarchy(hierarchy)
    # def add_hierarchies_set(self, hierarchies_set):
    #     for h in self._hierarchies_set:
    #         if h.name == hierarchies_set.name:
    #             raise Exception(
    #                 '''Hierarchies set "{0}" already exists in dimension
    #                 "{1}"'''.format(hierarchies_set.name, self.name))
    #     self._attributes.append(hierarchies_set)


class Attributes(SchemaElement):
    def __init__(self, name=None):
        super(Attributes, self).__init__(name=name)
        self._attributes = []


class Attribute(SchemaElement):
    def __init__(self, name, has_hierarchy='false',
                 key_column=None, level_type=None,
                 attr_key=None, attr_name=None):
        super(Attribute, self).__init__(name=name)
        if key_column:
            self._key_column = Property('keyColumn', key_column)
        self._has_hierarchy = Property('hasHierarchy', has_hierarchy)
        if level_type:
            self._level_type = Property('levelType', level_type)
        self._keys = []
        self._names = []

        if attr_key:
            self.add_keys(attr_key)
        if attr_name:
            self.add_names(attr_name)

    def add_keys(self, key):
        if not key.name:
            key.name = 'primary'
        for k in self._keys:
            if k.name == key.name:
                raise Exception(
                    '''Key set "{0}" already exists in attribute
                    "{1}"'''.format(key.name, self.name))
        self._keys.append(key)

    def add_names(self, name):
        self._names.append(name)


class Key(SchemaElement):
    def __init__(self, name=None, columns=None):
        super(Key, self).__init__(name)
        self._columns = []
        if columns:
            for col_name in columns:
                self.add_columns(Column(col_name))

    def add_columns(self, column):
        for c in self._columns:
            if c.name == column.name:
                raise Exception(
                    '''Column set "{0}" already exists in key
                    "{1}"'''.format(column.name, self.name))
        self._columns.append(column)

class Name(SchemaElement):
    def __init__(self, name=None, columns = None):
        super(Name, self).__init__(name=name)
        self._columns = []
        if columns:
            for col_name in columns:
                self.add_columns(Column(col_name))

    def add_columns(self, column):
        for c in self._columns:
            if c.name == column.name:
                raise Exception(
                    '''Column set "{0}" already exists in Name
                    "{1}"'''.format(column.name, self.name))
        self._columns.append(column)


class Column(SchemaElement):
    def __init__(self, name):
        super(Column, self).__init__(name=name)


class Level(SchemaElement):
    def __init__(self, attribute, name=None, column=None, name_column=None, visible=True,
                 level_type=None, approx_row_count=None, table=None,
                 ordinal_column=None, parent_column=None, ttype=None,
                 null_parent_value=None, internal_type=None, formatter=None,
                 unique_members=False, hide_member_if=None, caption=None,
                 description=None, caption_column=None):
        SchemaElement.__init__(self, name)
        self._attribute = Property('attribute', attribute)
        self._column = Property('column', column)
        self._name_column = Property('nameColumn', name_column)
        self._level_type = Property('levelType', level_type)
        self._approx_row_count = Property('approxRowCount', approx_row_count)
        self._table = Property('table', table)
        self._ordinal_column = Property('ordinalColumn', ordinal_column)
        self._parent_column = Property('parentColumn', parent_column)
        self._type = Property('type', ttype)
        self._null_parent_value = Property('nullParentValue',
                                            null_parent_value)
        self._internal_type = Property('internalType', internal_type)
        self._formatter = Property('formatter', formatter)
        self._unique_members = Property('uniqueMembers', unique_members)
        self._hide_member_if = Property('hideMemberIf', hide_member_if)
        self._caption = Property('caption', caption)
        self._description = Property('description', description)
        self._caption_column = Property('captionColumn', caption_column)

    @property
    def column(self):
        return self._column

    @column.setter
    def column(self, column):
        self._column.value = column

    @property
    def name_column(self):
        return self._name_column

    @name_column.setter
    def name_column(self, name_column):
        self._name_column.value = name_column

    @property
    def level_type(self):
        return self._level_type

    @level_type.setter
    def level_type(self, level_type):
        self._level_type.value = level_type

    @property
    def approx_row_count(self):
        return self._approx_row_count

    @approx_row_count.setter
    def approx_row_count(self, approx_row_count):
        self._approx_row_count.value = approx_row_count

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table):
        self._table.value = table

    @property
    def ordinal_column(self):
        return self._ordinal_column

    @ordinal_column.setter
    def ordinal_column(self, ordinal_column):
        self._ordinal_column.value = ordinal_column

    @property
    def parent_column(self):
        return self._parent_column

    @parent_column.setter
    def parent_column(self, parent_column):
        self._parent_column.value = parent_column

    @property
    def ttype(self):
        return self._type

    @ttype.setter
    def ttype(self, ttype):
        self._type.value = ttype

    @property
    def null_parent_value(self):
        return self._null_parent_value

    @null_parent_value.setter
    def null_parent_value(self, null_parent_value):
        self._null_parent_value.value = null_parent_value

    @property
    def internal_type(self):
        return self._internal_type

    @internal_type.setter
    def internal_type(self, internal_type):
        self._internal_type.value = internal_type

    @property
    def formatter(self):
        return self._formatter

    @formatter.setter
    def formatter(self, formatter):
        self._formatter.value = formatter

    @property
    def unique_members(self):
        return self._unique_members

    @unique_members.setter
    def unique_members(self, unique_members):
        self._unique_members.value = unique_members

    @property
    def hide_member_if(self):
        return self._hide_member_if

    @hide_member_if.setter
    def hide_member_if(self, hide_member_if):
        self._hide_member_if.value = hide_member_if

    @property
    def caption(self):
        return self._caption

    @caption.setter
    def caption(self, caption):
        self._caption.value = caption

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description.value = description

    @property
    def caption_column(self):
        return self._caption_column

    @caption_column.setter
    def caption_column(self, caption_column):
        self._caption_column.value = caption_column


class Measure(SchemaElement):
    def __init__(self, name, column=None, aggregator='sum', datatype=None,
                 format_string=None, formatter=None, caption=None,
                 description=None, visible=True):
        SchemaElement.__init__(self, name)
        self._column = Property('column', column)
        self._aggregator = Property('aggregator', aggregator)
        self._datatype = Property('datatype', datatype)
        self._format_string = Property('formatString', format_string)
        self._formatter = Property('formatter', formatter)
        self._caption = Property('caption', caption)
        self._description = Property('description', description)
        self._visible = Property('visible', visible)

    @property
    def column(self):
        return self._column

    @column.setter
    def column(self, column):
        self._column.value = column

    @property
    def aggregator(self):
        return self._aggregator

    @aggregator.setter
    def aggregator(self, aggregator):
        self._aggregator.value = aggregator

    @property
    def datatype(self):
        return self._datatype

    @datatype.setter
    def datatype(self, datatype):
        self._datatype.value = datatype

    @property
    def format_string(self):
        return self._format_string

    @format_string.setter
    def format_string(self, format_string):
        self._format_string.value = format_string

    @property
    def formatter(self):
        return self._formatter

    @formatter.setter
    def formatter(self, formatter):
        self._formatter.value = formatter

    @property
    def caption(self):
        return self._caption

    @caption.setter
    def caption(self, caption):
        self._caption.value = caption

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description.value = description

    @property
    def visible(self):
        return self._visible

    @visible.setter
    def visible(self, visible):
        self._visible.value = visible
