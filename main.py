import argparse
from core import Spectroman


__version__ = '2.0.0'
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='SPECTROMAN - Spectral Manager for SPECTROSED '
                    'Fetch data over a FTP server and implements pre and post-processing routines.')
    
    parser.add_argument("-n", "--network", 
                        help="Run SPECTROMAN over a FTP and feed the collected data to a DB",
                        action='store_true')
    
    parser.add_argument('-p', '--process',
                        help='Run in processing mode', 
                        action='store_true')
    
    parser.add_argument('-l', '--local', 
                        help='Run in local download mode', 
                        action='store_true')
    
    parser.add_argument('-v', '--version', 
                        help='Displays current package version.', 
                        action='store_true')
    
    # Converts the input arguments from Namespace() to dict
    args = parser.parse_args().__dict__
    
    # Instantiate a Spectroman class object to control app behaviour and logging
    manager = Spectroman(input_arguments=args)

    if args['version']:
        manager.log.info(f'SPECTROMAN version: {__version__}')
    elif args['network']:
        manager.option.network_mode()
    elif args['process']:
        manager.option.processing_mode()
    elif args['local']:
        manager.option.download_mode()
