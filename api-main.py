"""

@author: Danny Smyda
@date: 11-17-16

"""

import processor
import functions
import sys
import argparse

def main(argv):

    #Map each type to a the correct api url, makes it easier via command line to specify which call.
    API_URLS = {'perm': 'https://di-api.drillinginfo.com/v1/direct-access/permits?state_province={}&format=json&page={}&pagesize={}',
                'prodh': 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities?state_province={}&format=json&page={}&pagesize={}',
                'prodm': 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities-details?entity_id={}&format=json&page={}&pagesize={}'
                }

    #Command line parser, arguments accepted
    parser = argparse.ArgumentParser(description="Process Drilling Info API calls.")
    parser.add_argument('-type', type=str, help="TYPE (perm=Permit,prodh=Prod.Header,prodm=Prod.Monthly)")
    parser.add_argument('-api_key', type=str, help="API key")
    parser.add_argument('-s', type=str, nargs='+', help="Target state")
    parser.add_argument('-pn', type=int, default=1, help="API page num (default=1)")
    parser.add_argument('-ps', type=int, default=100, help="API page size (default=100)")

    args = parser.parse_args()
    request_url = None

    if args.type and args.type in API_URLS:
        request_url = API_URLS[args.type]

    threads = processor.distribute(request_url, args.api_key, args.type, args.s, args.pn, args.ps)

    print("\nCollecting data for inputted state sequence...\n")
    print("This is an interactive tool to check the status of the current jobs.\n"
          "Type \"active_jobs\" to see what is still running. Command line will\n"
          "close automatically once all jobs have completed. Check the log files for\n"
          "any warnings or errors. Type \"exit\" to end the program prematurely.\n")
    command = ""
    while command != "exit":
        command = input("> ")
        if command.lower() == "active_jobs":
            status_matrix(threads)
        elif command.lower() == "exit":
            if len(threads) > 0 and status(threads):

                for i,thread in enumerate(threads):
                    if thread.is_alive():
                        thread.terminate()
                        print("Thread {} safely exiting...".format(i + 1))
                        thread.join()

            print("User ended program via command \"exit\"\n")
        elif command.startswith("kill"):
            try:
                thread_num = int(command.split(" ")[1]) - 1
            except ValueError:
                print("Please end the command with the thread number you wish to kill.\n"
                      "Use command \"active_jobs\" to see what is currently running.")
                continue

            if -1 < thread_num < len(threads):
                if threads[thread_num].is_alive():
                    threads[thread_num].terminate()
                    print("Thread {} safely exiting...".format(thread_num + 1))
                    threads[thread_num].join()
                else:
                    print("Thread {} is already dead.".format(thread_num + 1))
            else:
                print("Invalid thread number, out of bounds.")
        elif command.startswith("convert_to_csv"):
            functions.convert_to_csv(command.split(" ")[1])
        else:
            print("Command not recognized")

    print("Check the log files for any errors or warnings.")

def status(threads):
    """ If atleast one thread is alive, return True. Only returns false once all threads are not alive."""

    for thread in threads:
        if thread.is_alive():
            return True

    return False

def status_matrix(threads):

    for i,thread in enumerate(threads):
        if thread.is_alive():
            print("{:>5}:{} with {} completed requests".format(i+1,thread.getName(), thread.getProgress()))

if __name__ == "__main__":
    main(sys.argv[1:])