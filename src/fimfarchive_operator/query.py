from lark import Lark, Transformer, v_args
import ast
from datetime import datetime


query = r"""
%import common.WS
%ignore WS
%import common.SIGNED_NUMBER
%import common.ESCAPED_STRING
%import common.CNAME

?query : negation
       | intersection
       | union
       | tag
       | grouped
       | comparison

negation : "-" grouped
         | "-" tag

tag : pattern

?pattern : PATTERN        -> partial_string
         | ESCAPED_STRING -> esc_string

PATTERN : /\w[\w ]*/

intersection : query "," query
union :  query "|" query

?grouped : "(" query ")"

comparison : feature_shift comparator feature_shift

?feature_list : feature_shift
              | feature_shift "," feature_list

column : COLUMN_NAME

COLUMN_NAME : ("." CNAME)+

?feature_atom : feature
              | "(" feature_shift ")"
              | SIGNED_NUMBER -> number
              | ESCAPED_STRING -> string

?feature_exp : feature_atom
              | feature_exp OPERATOR_EXP feature_atom -> feature_op
?feature_scale : feature_exp
                | feature_scale OPERATOR_SCALE feature_exp -> feature_op
?feature_shift : feature_scale
          | feature_atom OPERATOR_SHIFT feature_scale -> feature_op

operator : OPERATOR_EXP | OPERATOR_SCALE | OPERATOR_SHIFT
comparator : COMPARATOR -> operator

COMPARATOR : "<" | ">" | "<=" | ">=" | "=" | "=="
OPERATOR_EXP : "^"
OPERATOR_SCALE : "*" | "/"
OPERATOR_SHIFT : "+" | "-"

"""


def convert(x):
    if x == None:
        return 0
    if isinstance(x, str):
        return datetime.fromisoformat(x).timestamp()
    return x


OPERATORS = {
    "+": lambda x, y: x + y,
    "-": lambda x, y: x - y,
    "*": lambda x, y: x * y,
    "/": lambda x, y: x / y,
    "^": lambda x, y: x**y,
    ">": lambda x, y: convert(x) > convert(y),
    ">=": lambda x, y: convert(x) >= convert(y),
    "<": lambda x, y: convert(x) < convert(y),
    "<=": lambda x, y: convert(x) <= convert(y),
    "=": lambda x, y: convert(x) == convert(y),
    "==": lambda x, y: convert(x) == convert(y),
}


def get_field(key, data):
    for field in key.split(".")[1:]:
        data = data[field]
    return data


DEFAULT_CUSTOMIZATION = r"""
?feature : column
"""


@v_args(inline=True)
class QueryFilter(Transformer):
    def __init__(
        self, query_customization=DEFAULT_CUSTOMIZATION, tags_fn=lambda x: set()
    ):
        grammar = f"{query}\n{query_customization}"
        self.tags_fn = tags_fn
        self.query_parser = Lark(grammar, start="query")

    def __call__(self, query_string):
        parse_tree = self.query_parser.parse(query_string)
        return self.transform(parse_tree)

    def tag(self, pattern):
        return lambda x: any(map(lambda y: pattern(y), self.tags_fn(x)))

    def esc_string(self, string):
        return lambda x: x == ast.literal_eval(string.strip())

    def partial_string(self, partial):
        return lambda x: partial.strip() in x

    def negation(self, child):
        return lambda x: not child(x)

    def intersection(self, left, right):
        return lambda x: left(x) and right(x)

    def union(self, left, right):
        return lambda x: left(x) or right(x)

    def operator(self, op):
        return OPERATORS[op]

    def comparison(self, left_fn, operator, right_fn):
        return lambda x: operator(left_fn(x), right_fn(x))

    def feature_list(self, *args):
        return args

    def number(self, value):
        n = float(value)
        return lambda x: n

    def string(self, value):
        value = ast.literal_eval(value)
        return lambda x: value

    def feature_op(self, left_fn, operator, right_fn):
        op_fn = OPERATORS[operator]
        return lambda x: op_fn(left_fn(x), right_fn(x))

    def column(self, key_path):
        return lambda x: x[key_path[1:]]
