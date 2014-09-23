from django.db.models import F, ForeignKey
from django.contrib.contenttypes.generic import GenericForeignKey
from django.core.exceptions import ImproperlyConfigured

from django_model_changes import post_change
from django_model_changes.changes import registry

from .fields import CounterField

counters = {}


class Counter(object):
    """
    Counter keeps the CounterField counter named *counter_name* up to
    date. Whenever changes are made to instances of the counted child
    model, i.e. the model that defines the foreign field
    *foreign_field*, the counter is potentially incremented/decremented.
    A optional callback function *is_in_counter* can be supplied for
    control over exactly which child model instances are to be counted.
    By default, all non-deleted instances are counted.
    """
    def __init__(self, counter_name, foreign_field, is_in_counter=None, parent_model=None, child_model=None):
        self.counter_name = counter_name

        if isinstance(foreign_field, GenericForeignKey):
            if not parent_model:
                raise ImproperlyConfigured('%s is a GenericForeignKey field so it needs a parent_model to be specified'
                      % str(foreign_field))
            self.child_model = child_model if child_model else foreign_field.model
            self.foreign_field = self.child_model._meta.get_field_by_name(foreign_field.fk_field)[0]
            self.parent_model = parent_model
        elif foreign_field.field and isinstance(foreign_field.field, ForeignKey):
            self.foreign_field = foreign_field.field
            self.child_model = child_model if child_model else self.foreign_field.model
            self.parent_model = parent_model if parent_model else self.foreign_field.rel.to
        else:
            raise TypeError("%s should be a ForeignKey or GenericForeignKey based field but is %s" % (
                str(foreign_field), type(foreign_field)))

        if not is_in_counter:
            is_in_counter = lambda instance: True
        self.is_in_counter = is_in_counter

        self.connect()


    def validate(self):
        """
        Validate that this counter is indeed defined on the parent
        model.
        """
        counter_field, _, _, _ = self.parent_model._meta.get_field_by_name(
            self.counter_name
        )
        if not isinstance(counter_field, CounterField):
            raise TypeError("%s should be a CounterField on %s, but is %s" % (
                self.counter_name, self.parent_model, type(counter_field)))

    def has_parent_changed(self, new, old):
        return self.parent_id(old) != self.parent_id(new)

    def receive_change(self, instance, changes):
        """
        Called when child model instances are saved/destroyed.
        Increments/decrements the underlying counter based on whether
        the child was/is in the counter.
        """

        old_instance = changes.old_instance()

        changed = self.has_parent_changed(instance, old_instance)

        was_in_counter = changes.was_persisted() and \
                         self.is_in_counter(old_instance)
        is_in_counter = changes.is_persisted() and \
                        self.is_in_counter(instance)

        if is_in_counter:
            if changed or not was_in_counter:
                self.increment(instance, 1)

        if was_in_counter:
            if changed:
                self.increment(old_instance, -1)
            elif not is_in_counter:
                self.increment(instance, -1)

    def connect(self):
        """
        Register a counter between a child model and a parent.
        """
        self.validate()
        registry.register(self.child_model)

        def receiver(sender, instance, changes, **kwargs):
            self.receive_change(instance, changes)
        post_change.connect(receiver, sender=self.child_model, weak=False)

        name = "%s.%s.%s" % (
            self.parent_model._meta.model_name,
            self.child_model._meta.model_name,
            self.foreign_field.name
        )
        counted_name = "%s-%s" % (name, self.counter_name)
        counters[counted_name] = self

    def parent_id(self, child):
        """
        Returns the id of the parent that includes the given *child*
        instance in its counter.
        """
        return getattr(child, self.foreign_field.attname)

    def set_counter_field(self, parent_id, value):
        """
        Set the value of a counter field on *parent_id* to *value*.
        """
        return self.parent_model.objects.filter(pk=parent_id).update(**{
            self.counter_name: value
        })

    def increment(self, child, amount):
        """
        Increment a counter using a *child* instance to find the the
        parent. Pass a negative amount to decrement.
        """
        parent_id = self.parent_id(child)
        return self.set_counter_field(parent_id, F(self.counter_name)+amount)


def connect_counter(counter_name, foreign_field, is_in_counter=None, parent_model=None, child_model=None):
    """
    Register a counter between a child model and a parent. The parent
    must define a CounterField field called *counter_name* and the child
    must reference its parent using a ForeignKey *foreign_field*. Supply
    an optional callback function *is_in_counter* for over which child
    instances to count.
    By default, all persisted (non-deleted) child instances are counted.

    Arguments:
    counter_name - The name of the counter. A CounterField field with
    this name must be defined on the parent model.
    foreign_field - A ForeignKey field defined on the counted child
    model. The foreign key must reference the parent model.
    is_in_counter - The callback function is_in_counter will be given
    instances of the counted model. It must return True if the instance
    qualifies to be counted, and False otherwise. The callback should
    not concern itself with checking if the instance is deleted or not.
    """
    return Counter(counter_name, foreign_field, is_in_counter=is_in_counter, parent_model=parent_model, child_model=child_model)
