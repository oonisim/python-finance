import argparse

SEC_FORM_TYPE_10K = "10-K"
SEC_FORM_TYPE_10Q = "10-Q"

parser = argparse.ArgumentParser(description='EDGAR program')
parser.add_argument(
    '-f', '--form-types', type=str.upper, nargs='+', required=False,
    default=["10-K", "10-Q"],
    help='specify the form types to select e.g. 10-Q, 10-K'
)
parser.add_argument(
    '-t', '--test-mode', action="store_true",
    help='specify to use the test mode'
)

args = vars(parser.parse_args())
print(args)

# Validations
assert all([
    form in [SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K]
    for form in args['form_types']
]), "Invalid form type(s) in [%s]" % args['form_types']

assert isinstance(args["test_mode"], bool), f"Invalid type {type(args['test_mode'])}"

