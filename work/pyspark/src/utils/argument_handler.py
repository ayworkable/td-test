from typing import List
import sys


class ArgumentHandler:
    def __init__(self, argv: List[str]):
        self.argv = argv

    def validate_required_config_argv(self) -> None:
        sys_argv = self.get_sys_args()
        if len(sys_argv) != len(self.argv):
            raise ValueError(
                f"Expect {len(self.argv)} arguments: [{', '.join(self.argv)}]: now getting {sys_argv}"
            )

    def get_sys_args(self) -> List[str]:
        return sys.argv[1:len(sys.argv)]
