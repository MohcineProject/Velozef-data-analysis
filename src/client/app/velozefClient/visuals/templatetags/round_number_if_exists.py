from django import template ; 

register = template.Library() ; 


@register.filter
def round_number_if_exists(value):
    if isinstance(value, float):
        return round(value, 2)
    return value
