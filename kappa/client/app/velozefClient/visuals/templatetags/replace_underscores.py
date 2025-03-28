from django import template

register = template.Library()

@register.filter
def replace_underscores(value):
    if isinstance(value, str):
        return value.replace('_', ' ')
    return value
