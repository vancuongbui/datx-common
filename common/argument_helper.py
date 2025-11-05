import argparse
import datetime
import typing


def make_action(parse_func: typing.Callable[[str], typing.Any]):
    class CustomAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            parsed_value = parse_func(values)
            setattr(namespace, self.dest, parsed_value)

    return CustomAction


ToUppercaseAction = make_action(lambda value: value.upper())
ToDateAction = make_action(lambda value: datetime.date.fromisoformat(value))
