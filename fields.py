from collections import namedtuple
from copy import copy
from psycopg2.extensions import adapt, register_adapter, ISQLQuote

from django.db import models, connections
from django.db.models import Q, get_model, FieldDoesNotExist


DEFAULT_WEIGHT = 'D'


class Vector(object):
    """
    The VectorField implementation must add the model value
    to all of its VQ.children, also config
    """
    model = None

    def __init__(self, lookup, config=None, weight=DEFAULT_WEIGHT):
        self.lookup = lookup
        self.config = config
        self.weight = weight

    @property
    def field(self):
        if self.model is None:
            raise ValueError('No model applied yet')
        parts = self.lookup.split('__')
        obj = self.model
        field = None
        for part in parts:
            field = obj._meta.get_field(part)
            try:
                obj = field.rel.to
            except AttributeError:
                break
        return field


class VQ(Q):
    """
    VQ('market__language') | VQ(product__name='A')
    """
    AND = '&&'
    OR = '||'
    default = OR

    def __init__(self, *args, **kwargs):
        vectors = [Vector(arg) for arg in args]
        vectors += [Vector(key, weight=val) for key, val in kwargs.items()]
        super(VQ, self).__init__(*vectors)

    def __str__(self):
        """
        From Django tree.Node but using !! instead of NOT
        """
        if self.negated:
            return '(!! (%s: %s))' % (
                self.connector, ', '.join([str(c) for c in self.children]))
        return '(%s: %s)' % (
            self.connector, ', '.join([str(c) for c in self.children]))


class VectorQuoter(ISQLQuote):
    def prepare(self, conn):
        qn = conn.ops.quote_name
        self.table = qn(self._wrapped.model._meta.db_table)
        self.column = qn(self._wrapped.field.column)

    def getquoted(self):
        return "setweight(to_tsvector('{config}', coalesce({table}.{column}, '')), '{weight}')".format(
            config=self._wrapped.config,
            table=self.table,
            column=self.column,
            weight=self._wrapped.weight,
        )


class VQQuoter(ISQLQuote):
    def __init__(self, vq):
        self._vq = copy(vq)
        self._conn = None

    def prepare(self, conn):
        self._conn = conn

    def getquoted(self):
        adapted_vectors = [adapt(v) for v in self._vq.children]
        if self._conn is not None:
            for obj in adapted_vectors:
                if hasattr(obj, 'prepare'):
                    obj.prepare(self._conn)
        quoted_vectors = [o.getquoted() for o in adapted_vectors]
        self._vq.children = quoted_vectors
        return str(self._vq)


def adapt_vector(vector):
    return VectorQuoter(vector)


def adapt_vq(vq):
    return VQQuoter(vq)


register_adapter(Vector, adapt_vector)
register_adapter(VQ, adapt_vq)


class VectorField(models.Field):
    """
    TODO:
    fulltext = VectorField(
        content=VQ('market__language') | VQ(product__name='A'),
        config='pg_catalog.english'
    )
    fulltext = VectorField(
        content=VQ('market__language') | VQ(product__name='A'),
        config=get_config
    )
    """
    def __init__(self, content, config='pg_catalog.english', *args, **kwargs):
        kwargs['null'] = True
        kwargs['default'] = ''
        kwargs['editable'] = False
        kwargs['serialize'] = False
        kwargs['db_index'] = True
        self.content = content
        self.config = config
        super(VectorField, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name):
        super(VectorField, self).contribute_to_class(cls, name)
        # finish setting up VQ objects
        for vq in self.content.children:
            vq.model = self.model
            if vq.config is None:
                vq.config = self.config

    def db_type(self, *args, **kwargs):
        return 'tsvector'

    #def get_prep_lookup(self, lookup_type, value):
    #    if hasattr(value, 'prepare'):
    #        return value.prepare()
    #    if hasattr(value, '_prepare'):
    #        return value._prepare()
    #    raise TypeError("Field has invalid lookup: %s" % lookup_type)

    def get_db_prep_lookup(self, lookup_type, value, connection, prepared=False):
        return self.get_prep_lookup(lookup_type, value)

    def get_prep_value(self, value):
        return value

try:
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules(rules=[], patterns=['django-postgres-vectorfield\.fields\.VectorField'])
except ImportError:
    pass
