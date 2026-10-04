from agentbox_worker import main
from sim_worker import NAME, VERSION
from sim_worker.app import run

main(run, name=NAME, version=VERSION)
