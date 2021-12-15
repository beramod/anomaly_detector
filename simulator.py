from src.manager.simulator_manager import SimulatorManager

def main():
    simulator_manager = SimulatorManager()
    simulator_manager.run(00000, '211122000000', '211124230000')

if __name__ == '__main__':
    main()