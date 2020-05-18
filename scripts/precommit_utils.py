import sys
import os


def interpret_precommit_args():
    """Precommit passes its arguments strangely and this lets us figure out which ones are which type"""
    path_args = list()
    cli_args = list()
    for arg in sys.argv[1:]:
        if os.path.exists(arg):
            path_args.append(arg)
        else:
            cli_args.append(arg)

    if not path_args:
        raise Exception(
            "Everything passed was considered to be a CLI argument, "
            "so there was nothing to check. " + str(cli_args)
        )

    return path_args, cli_args
