if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition

@condition
def evaluate_condition(data,*args, **kwargs) -> bool:
    if data['rows'][0]>0:
        return True
    else:
        return False

