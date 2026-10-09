from agentbox_worker import main
from evalworker import NAME, VERSION
from evalworker.app import run

main(run, name=NAME, version=VERSION)
