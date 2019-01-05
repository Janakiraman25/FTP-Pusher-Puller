#!/usr/bin/python

from ftplib import FTP
import sys, os, signal, time, logging, json, ast, glob, threading

class Puller(object):

        def __init__(self):

                self.pwd = os.getcwd()
                config_file = (self.pwd+"/FTPInputData.json")

                if os.path.isfile(config_file):

                        print("Reading data from Config File")

                else:

                        print("FTPInputData.json file not found")
                        sys.exit(0)

                last_file = (self.pwd+"/LastFileProcessed.txt")

                if os.path.isfile(last_file):

                        print("Reading data from LastFileProcessed.txt")

                else:

                        print("LastFileProcessed.txt file not found")
                        sys.exit(0)

                try:

                        self.file = open("FTPInputData.json", 'r')
                        self.read_config = self.file.read()
                        self.dict = ast.literal_eval(self.read_config)
                        self.SM = self.dict["ScriptMethod"]
                        self.ST = self.dict["RetainFTPFile"]
                        self.IP = self.dict["HostName"]
                        self.USER = self.dict["Username"]
                        self.PASWD = self.dict["Password"]
                        self.RDIR = self.dict["RemoteDir"]
                        self.PRE = self.dict["FilePrefix"]
                        self.EXT = self.dict["FileExt"]
                        self.LGDIR = self.dict["LogDir"]
                        self.LDIR = self.dict["LocalDir"]
                        self.SL = self.dict["SleepTime"]
                        self.SLEEP = float(self.SL)
                        self.TO = int(self.SLEEP + 1000)
                        self.file.close()

                        if not bool(self.IP) and not bool(self.USER) and not bool(self.PASWD) and not bool(self.LGDIR) and not bool(self.LDIR) and not bool(self.SM) and not bool(self.SLEEP) and not bool(self.ST) and not bool(self.SL):
                                print ("Error in Config file. Hence Exiting the application")
                                sys.exit(0)

                except AttributeError as e:

                        print(e)
                        print("Attribute Error: Tag missing in config file")

                except SyntaxError as e:

                        print(e)
                        print("Syntax Error: Tag missing in config file")

        def remote_login(self):

                self.generate_logger()
                if self.SM == "1":

                        self.logger.info("ScriptMethod : Puller")

                else:

                        self.logger.info("ScriptMethod : Pusher")

                if self.ST == "1":

                        self.logger.info("RetainFTPFile : Enabled")

                else:

                        self.logger.info("RetainFTPFile : Disabled")

                self.logger.info("HostName : "+self.IP)
                self.logger.info("Username : "+self.USER)
                self.logger.info("Password : "+self.PASWD)
                self.logger.info("RemoteDir : "+self.RDIR)
                self.logger.info("FilePrefix : "+self.PRE)
                self.logger.info("FileExt : "+self.EXT)
                self.logger.info("LogDir : "+self.LGDIR)
                self.logger.info("LocalDir : "+self.LDIR)
                self.logger.info("SleepTime : "+self.SL)

                try:

                        self.ftp = FTP(self.IP, timeout=self.TO)
                        print("FTP Successfull")
                        self.logger.info("FTP Successfull")

                except Exception as e:

                        print(e)
                        print("Socket Error. Unable to connect to host")
                        self.logger.error("Socket Error. Unable to connect to host")
                        sys.exit(0)
                try:

                        self.ftp.login(user=self.USER, passwd=self.PASWD)
                        print("Login Successfull")
                        self.logger.info("Login Successfull")

                except Exception as e:

                        print(e)
                        print("Login credentials incorrect")
                        self.logger.error("Login credentials incorrect")
                        sys.exit(0)
                try:

                        self.ftp.set_pasv(True)
                        print("Set to Passive Mode")
                        self.logger.info("Set to Passive Mode")

                except Exception as e:

                        print(e)
                        print("Unable to set to passive mode")
                        self.logger.warning("Unable to set to passive mode")

                try:

                        self.ftp.sendcmd("TYPE i")
                        print("Switched to Binary mode")
                        self.logger.info("Switched to Binary mode")

                except Exception as e:

                        print(e)
                        print("Unable to set to Binary mode")
                        self.logger.warning("Unable to set to Binary mode")

                try:

                        self.ftp.cwd(self.RDIR)
                        self.logger.info("Changed Remote directory")

                except Exception as e:

                        print(e)
                        print("Unable to change Remote directory")
                        self.logger.error("Unable to change Remote directory")
                        self.ftp.quit()
                        sys.exit(0)

                try:

                        os.chdir(self.LDIR)
                        self.logger.info("Changed to Local Path")

                except Exception as e:

                        print(e)
                        print("Unable to change local directory")
                        self.logger.error("Unable to change local directory")
                        self.ftp.quit()
                        sys.exit(0)


        def pull_del(self):

                self.generate_logger()
                files_list = []
                ext = self.PRE+"*"+self.EXT
                files_list = self.ftp.nlst(ext)

                try:

                        files_list.pop()

                except Exception as e:

                        print(e)
                        self.logger.error("Nothing to Pop due to empty list.")
                        self.ftp.quit()
                        sys.exit(0)

                len_files_list = str(len(files_list))
                self.logger.debug("Total files to pull "+len_files_list)

                if files_list:

                        for i in files_list:

                                try:

                                        self.localfile = open(i, 'wb')
                                        self.ftp.retrbinary('RETR ' + i, self.localfile.write, 1024)
                                        write_file = open(self.pwd+"/LastFileProcessed.txt", 'w')
                                        write_file.write(i)
                                        write_file.close()
                                        self.logger.debug("Pulled "+i+" file to Local Path")

                                        try:

                                                self.ftp.delete(i)
                                                self.localfile.close()
                                                self.logger.debug("Deleted "+i+" file from Remote Path")

                                        except Exception as e:

                                                print(e)
                                                self.logger.error("Unable to delete files from Remote Path")
                                                self.ftp.quit()
                                                sys.exit(0)

                                except Exception as e:

                                        print(e)
                                        self.logger.error("Unable to Pull files from Remote Path")
                                        self.ftp.quit()
                                        sys.exit(0)

                else:
                        self.logger.warning("No new files to process. Application going to sleep mode")
                        time.sleep(self.SLEEP)


        def pull_ret(self):

                self.generate_logger()
                files_list = []
                ext = self.PRE+"*"+self.EXT
                files_list = self.ftp.nlst(ext)
                last_file = open(self.pwd+"/LastFileProcessed.txt", 'r')
                read_file = last_file.read()
                last_file.close()

                try:

                        replace_n = read_file.replace('\n', '')
                        index_get = files_list.index(replace_n)
                        del files_list[:index_get+1]

                        try:

                                files_list.pop()
                        except Exception as e:

                                print(e)
                                self.logger.error("Nothing to Pop due to empty list.")
                                self.ftp.quit()
                                sys.exit(0)

                        len_files_list = str(len(files_list))
                        self.logger.debug("Total files to pull "+len_files_list)

                        if files_list:

                                for i in files_list:

                                        try:

                                                self.localfile = open(i, 'wb')
                                                self.ftp.retrbinary('RETR ' + i, self.localfile.write, 1024)
                                                self.localfile.close()
                                                write_file = open(self.pwd+"/LastFileProcessed.txt", 'w')
                                                write_file.write(i)
                                                write_file.close()
                                                self.logger.debug("Pulled "+i+" file to Local Path")

                                        except Exception as e:

                                                print(e)
                                                self.logger.error("Unable to Pull files from Remote Path")
                                                self.ftp.quit()
                                                sys.exit(0)

                        else:
                                self.logger.warning("No new files to process. Application going to sleep mode")
                                time.sleep(self.SLEEP)

                except Exception as e:

                        print(e)
                        self.logger.error("Null/Wrong value in LastFileProcessed.txt file")
                        self.ftp.quit()
                        sys.exit(0)


        def push_del(self):

                self.generate_logger()
                files_list = []
                ext = self.PRE+"*"+self.EXT
                files_list = glob.glob(ext)

                try:

                        files_list.pop(0)

                except Exception as e:

                        print(e)
                        self.logger.error("Nothing to Pop due to empty list.")
                        self.ftp.quit()
                        sys.exit(0)

                len_files_list = str(len(files_list))
                self.logger.debug("Total files to push "+len_files_list)

                if files_list:

                        for i in files_list:

                                try:

                                        self.localfile = open(i, 'rb')
                                        self.ftp.storbinary("STOR " + i, self.localfile)
                                        write_file = open(self.pwd+"/LastFileProcessed.txt", 'w')
                                        write_file.write(i)
                                        write_file.close()
                                        self.logger.debug("Pushed "+i+" file to Remote Path")

                                        try:

                                                os.remove(i)
                                                self.localfile.close()
                                                self.logger.debug("Deleted "+i+" file in Local Path")

                                        except Exception as e:

                                                print(e)
                                                self.logger.error("Unable to delete file in Local Path")
                                                self.ftp.quit()
                                                sys.exit(0)

                                except Exception as e:

                                        print(e)
                                        self.logger.error("Unable to push file to Remote Path")
                                        self.ftp.quit()
                                        sys.exit(0)

                else:

                        self.logger.warning("No new files to process. Application going to sleep mode")
                        time.sleep(self.SLEEP)


        def push_ret(self):

                self.generate_logger()
                files_list = []
                ext = self.PRE+"*"+self.EXT
                files_list = glob.glob(ext)
                last_file = open(self.pwd+"/LastFileProcessed.txt", 'r')
                read_file = last_file.read()
                last_file.close()

                try:

                        replace_n = read_file.replace('\n', '')
                        index_get = files_list.index(replace_n)
                        del files_list[:index_get+1]

                        try:

                                files_list.pop(0)

                        except Exception as e:

                                print(e)
                                self.logger.error("Nothing to Pop due to empty list.")
                                self.ftp.quit()
                                sys.exit(0);

                        len_files_list = str(len(files_list))
                        self.logger.debug("Total files to push "+len_files_list)

                        if files_list:

                                for i in files_list:

                                        try:

                                                self.localfile = open(i, 'rb')
                                                self.ftp.storbinary("STOR " + i, self.localfile)
                                                self.localfile.close()
                                                write_file = open(self.pwd+"/LastFileProcessed.txt", 'w')
                                                write_file.write(i)
                                                write_file.close()
                                                self.logger.debug("Pushed "+i+" file to Remote Path")

                                        except Exception as e:

                                                print(e)
                                                self.logger.error("Unable to Push file to Remote Path")
                                                self.ftp.quit()
                                                sys.exit(0)

                        else:

                                self.logger.warning("No new files to process. Application going to sleep mode")
                                time.sleep(self.SLEEP)

                except Exception as e:

                        print(e)
                        self.logger.error("Null/Wrong value in LastFileProcessed.txt file")
                        self.ftp.quit()
                        sys.exit(0)


        def generate_logger(self):

                self.logger = logging.getLogger()
                if not self.logger.handlers:
                        timestr = time.strftime("%Y_%m_%d")
                        self.log_file = self.LGDIR+'/FTP_MEDIATION_' + timestr + '.log'
                        open(self.log_file, "w+")
                        self.logger.setLevel(logging.DEBUG)
                        self.fh = logging.FileHandler(self.log_file)
                        self.fh.setLevel(logging.DEBUG)
                        self.ch = logging.StreamHandler()
                        self.ch.setLevel(logging.DEBUG)
                        self.formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
                        if self.logger.handlers:
                                self.logger.handlers = []
                        self.ch.setFormatter(self.formatter)
                        self.fh.setFormatter(self.formatter)
                        self.logger.addHandler(self.ch)
                        self.logger.addHandler(self.fh)
                return self.logger

        def signal_handler(self, sig, frame):

                print("Exiting the Application")
                self.logger.critical("Received SIGUSR from user. Hence exiting the application")
                self.ftp.quit()
                self.t1.terminate()
                sys.exit(0)

        def ThreadFunction1(self):

                self.t1 = threading.Thread(target=self.pull_ret)
                self.t1.start()
                self.t1.join(5.0)

        def ThreadFunction2(self):

                t2 = threading.Thread(target=self.pull_del)
                t2.setDaemon(True)
                t2.start()
                t2.join()

        def ThreadFunction3(self):

                t3 = threading.Thread(target=self.push_del)
                t3.setDaemon(True)
                t3.start()
                t3.join()

        def ThreadFunction4(self):

                t4 = threading.Thread(target=self.push_ret)
                t4.setDaemon(True)
                t4.start()
                t4.join()

        def main(self):

                self.generate_logger()
                self.remote_login()
                signal.signal(2, self.signal_handler)


if __name__ == "__main__":

        class_call = Puller()
        class_call.main()

        if class_call.SM == "1" and class_call.ST == "1":

                while True:

                        class_call.ThreadFunction1()

        elif class_call.SM == "1" and class_call.ST == "0":

                while True:

                        class_call.ThreadFunction2()

        elif class_call.SM == "0" and class_call.ST == "0":

                while True:

                        class_call.ThreadFunction3()

        elif class_call.SM == "0" and class_call.ST == "1":

                while True:

                        class_call.ThreadFunction4()

        else:

                print("Error in Config Values. Hence exiting application")
                sys.exit();
