import pandas as pd
from datetime import datetime


class AnalyseLogs:
    Key_words = ['STARTED - TIME', 'ENDED - TIME', 'ENDED BY CC 0010 - TIME', 'USERID', '$HASP373', 'CLASS']
    Liblist = ['STEPLIB', 'DBRMLIB', 'LOADLIB']
    abend_list = ['ABEND=']
    key_result = list()
    Utility_result = list()
    condcode_result = list()
    final_result = list()
    abend_result = list()
    reason_list = list()
    total_steps = list()
    Lib_result = list()
    Temp_result = list()
    res = list()
    cpu = list()
    temp_word = temp_string = file_name = ''
    step_block = list()
    temp_lib_res = list()
    more_abend_info = list()
    temp_ab_res = list()
    temp_ab_res1 = list()
    ab_data = list()
    ab_list_data = list()
    job_info = list()
    temp_job_info = list()
    jes_df = pd.read_excel("Solution Catalog.xlsx", dtype=str)
    jescodes = jes_df["Token Identifier"].tolist()

    def extract_keyword(self, data_list):

        # print('EXTRACT KEYWORD FUNTION')
        # print(self.Key_words)
        for line in data_list:
            line = line.rstrip('\n')

            for i in self.Key_words:
                if line.__contains__(i):
                    index = line.find(i)
                    if index != -1:
                        temp_string = line[index + len(i) + 1:]
                        for char in temp_string:
                            if char != ' ':
                                self.temp_word += char
                            else:
                                break
                    self.key_result.append(self.temp_word)
                    self.temp_word = ''
            if line.__contains__('$HASP373'):
                temp_word = line[10:20]
                self.key_result.append(temp_word)
            self.temp_word = ' '
            if line.strip() == '0------ JES2 JOB STATISTICS ------':
                break

        start_time = self.key_result[4]
        end_time = self.key_result[5]
        # print( start_time )
        # print( end_time )
        fmt = ' %H.%M.%S'
        runtime = datetime.strptime(end_time, fmt) - datetime.strptime(start_time, fmt)
        # print( tdelta )
        self.key_result.append(runtime)
        for line in data_list:
            line = line.rstrip('\n')
            if line.__contains__('CPU:'):
                start = line.find("CPU:") + len("CPU:")
                end = line.find("SRB:")
                self.cpu = line[start:end]

        self.cpu = self.cpu.replace(" HR  ", ":")
        self.cpu = self.cpu.replace(" MIN  ", ":")
        self.cpu = self.cpu.replace(" SEC", " ")
        print(self.cpu)
        self.key_result.append(self.cpu)
        # print(self.key_result)

    def extarct_steps(self, data_list):

        # print('extarct_steps function')
        d = list()
        for line in data_list:
            line = line.rstrip('\n')
            if line.__contains__('EXEC PGM'):
                d.append(line)
        self.total_steps = self.total_steps + d
        # print(self.total_steps)

    def extract_utility(self, data_list):

        # print('extract_utility')
        for line in data_list:
            line = line.rstrip('\n')
            if line.__contains__('EXEC PGM'):
                index = line.find('EXEC PGM')
                # print(index)
                # print(len('EXEC PGM'))
                if index != -1:

                    temp_string = line[index + len('EXEC PGM') + 1:]
                    for char in temp_string:
                        if char != ',':
                            self.temp_word += char
                        else:
                            break
                self.Utility_result.append(self.temp_word)
                self.temp_word = ''
        # print(self.Utility_result)

    def extract_condcode(self, data_list):

        # print('Extract condcode function')
        k = []
        for line in data_list:
            line = line.rstrip('\n')
            if line.__contains__('COND CODE') or line.__contains__('COMPLETION CODE - SYSTEM'):
                k.append(line[-4:])
        self.condcode_result = self.condcode_result + k

        # print(self.condcode_result)

    def extract_lib(self, data_list):

        temp_lib_flag = False

        # print('extract LIB function')
        for line in data_list:
            line = line.rstrip('\n')
            for i in self.Liblist:
                index = line.find(i)
                if index != -1:
                    temp_string = line[index + len(i) + 1:]
                    for char in temp_string:
                        if char != ',':
                            self.temp_word += char
                            # print(self.temp_word)
                        else:
                            break

                    self.temp_lib_res.append(self.temp_word)
                    self.temp_word = ''
                    temp_lib_flag = True
        if temp_lib_flag:

            self.Lib_result.append(self.temp_lib_res)
        else:

            self.Lib_result.append('No system Libraries')

        # print(self.Lib_result)

    def step_division(self, data_list):

        # print('step_division function')
        data = list()
        flag = False
        for line in data_list:
            if line.__contains__('STEP WAS EXECUTED') or line.__contains__('COMPLETION CODE - SYSTEM'):
                flag = True
            if flag:
                data.append(line)
            if line.strip().__contains__('CPU:'):
                flag = False
                # break
                if not data:
                    break
                self.step_block.append(data)
                data = []
        self.extract_data_sets()

    def extract_data_sets(self):

        # print('extract data sets function')
        d = c = final = ''
        b = []
        for k in self.step_block:
            for line in k:

                line = line.rstrip('\n')
                if line.__contains__('IEF285I') and 'LOAD' not in line and line.__contains__('KEPT') \
                        or line.__contains__('CATALOGED') or line.__contains__('RETAINED'):
                    d = d + line

            for i in range(len(d)):
                if d[i] == ' ':
                    b.append(c)
                    c = ''
                else:
                    c = c + d[i]
            b.append(c)
            for j in range(len(b)):
                # print(b[j])
                if b[j] != 'IEF285I' and b[j] != 'KEPT' and b[j] != 'CATALOGED' and b[j] != '' and b[j] != 'RETAINED' \
                        and b[j] != 'IGD104I' and b[j] != 'DDNAME=DISK':
                    final = final + b[j] + ','

            self.final_result.append(final)
            final = c = d = ' '
            b = []
            self.step_block = []
        if not self.final_result:
            self.final_result.append('NO DATA SETS FOUND')
        # print(self.final_result)

    def extract_abend(self, data_list):

        # print('extract abend function')

        flag = False

        temp_word = ''
        # print('fun5')
        for line in data_list:

            line = line.rstrip('\n')

            for i in self.abend_list:

                if line.__contains__(i):

                    index = line.find(i)

                    if index != -1:

                        temp_string = line[index + len(i):]

                        for char in temp_string:

                            if char != ' ':
                                temp_word += char
                            else:
                                break

                        self.res.append(temp_word)
                        temp_word = ''
                        flag = True

        if flag:

            self.abend_result.append(self.res)
        else:
            self.abend_result.append('RUN SUCCESSFULL')

        # print(self.abend_result)
        self.res = []

    def extract_reason(self, data_list):

        # print('extract reason function')
        # print(self.jescodes)
        k = list()
        # a = ' '
        # cnt = 0
        flag = False
        for line in data_list:
            # cnt += 1
            # DSNU095I : DB2LOAD ERROR.
            # IEB311I : CONFLICTION IN DCB PARAMETERS, ABEND =S822 U0000
            for jes in self.jescodes:
                if line.__contains__(jes):
                    print('first if')
                    # print( line )
                    k.append(line)
                    flag = True

        if flag:
            self.reason_list.append(k)
        else:
            self.reason_list.append('NOT ABLE TO FIND REASON')
        # print(self.reason_list)

    def extract_abend_info(self, data_list):

        # print('more abend info')
        ab_flag = False
        for f in self.abend_result:

            for g in f:

                if g == 'S0C7':

                    # print('inside soc7 loop')

                    for line in data_list:

                        if line.__contains__('Invalid data'):
                            # print(line)
                            self.temp_ab_res.append(line)
                            ab_flag = True

                if g == 'S0CB':

                    print('inside SOCB loop')

                    for line in data_list:

                        line = line.rstrip('\n')

                        if line.__contains__('Local Variables:'):
                            ab_flag = True
                        if ab_flag:
                            self.ab_data.append(line)

                        if line.strip().__contains__('Run-Time Options Report:'):

                            ab_flag = False

                            if not self.ab_data:
                                break

                            self.temp_ab_res.append(self.ab_data)

                            ab_flag = True

                            self.ab_data = []
                if g == 'S04E':
                    for line in data_list:

                        if line.startswith(" DSNU"):
                            self.temp_ab_res.append(line)

                            ab_flag = True
        # print(self.temp_ab_res)

        if ab_flag:

            self.more_abend_info.append(self.temp_ab_res)

        else:

            self.more_abend_info.append('No More info')

        # print(self.more_abend_info)

    def extract_job_info(self, data_list):

        df_flag1 = False
        df_flag = False
        job_flag = False
        data_list.reverse()
        for line in data_list:
            self.temp_job_info.append(line)
            job_flag = True
            if line.find('PROCESSING COMPLETE') != -1 or line.find('CPU:') != -1:
                break
            else:
                job_flag = False

        self.temp_job_info.reverse()

        # print(self.job_info)

        if job_flag:
            self.job_info.append(self.temp_job_info)

        for a in range(len(self.abend_result)):
            # checking for abend
            if self.abend_result[a] == 'RUN SUCCESSFULL':
                df_flag = True

        for c in range(len(self.condcode_result)):

            # print(self.condcode_result[c])

            # checking for condcode (to list out in separate sheet)
            if self.condcode_result[c] == '0000':

                df_flag1 = True
            else:
                break

        if df_flag and df_flag1:

            # print('run success')

            df1 = pd.DataFrame(self.key_result)
            df1 = df1.transpose()
            df1.columns = ["OWNER", "JOB NAME", "JOB CLASS", "JOB ID", "STARTED_TIME", "ENDED_TIME", "RUN TIME", "CPU"]
            # print(df1)
            df2 = pd.DataFrame(self.total_steps)
            df2.columns = ["STEPS IN JCL"]
            df2['UTILITY/STEP NAME'] = self.Utility_result
            # print(df2)
            df3 = pd.DataFrame(self.condcode_result)
            df3.columns = ['COND CODES']
            # print(df3)
            df4 = pd.DataFrame(self.final_result)
            df4.columns = ['STEP WISE DATA SETS']
            # print(df4)
            df1['STEP AND DBRM LIBS'] = self.Lib_result
            df1['RUN STATUS'] = self.abend_result
            df1["JOB INFO"] = self.job_info
            # print(df1)
            df = pd.concat([df1, df2, df3, df4], axis=1)
            # print(df)
            print(df)
            df = df[['JOB NAME', 'OWNER', 'JOB CLASS', 'JOB ID', 'RUN TIME', 'CPU', 'STEPS IN JCL', 'UTILITY/STEP NAME',
                     'COND CODES', 'RUN STATUS', 'STEP WISE DATA SETS', 'STARTED_TIME', 'ENDED_TIME',
                     'STEP AND DBRM LIBS', 'JOB INFO']]

            df.to_csv(r'C:\Users\m1055990\Desktop\Lagacy.csv', index=False, mode='a', header=False)

        else:

            # print('Abended jobs')
            df5 = pd.DataFrame(self.key_result)
            df5 = df5.transpose()
            df5.columns = ["OWNER", "JOB NAME", "JOB CLASS", "JOB ID", "STARTED_TIME", "ENDED_TIME", "RUN TIME", "CPU"]
            df6 = pd.DataFrame(self.total_steps)
            df6.columns = ["STEPS IN JCL"]
            df6['UTILITY/STEP NAME'] = self.Utility_result
            df7 = pd.DataFrame(self.condcode_result)
            df7.columns = ['COND CODES']
            df8 = pd.DataFrame(self.final_result)
            df8.columns = ['STEP WISE DATA SETS']
            df5['STEP AND DBRM LIBS'] = self.Lib_result
            df5['RUN STATUS'] = self.abend_result
            df5['REASON FOR ABEND'] = self.reason_list
            df5['MORE ABEND INFO'] = self.more_abend_info
            df5['JOB INFO'] = self.job_info
            dataf = pd.concat([df5, df6, df7, df8], axis=1)
            # print(dataf)
            print(dataf)
            dataf = dataf[['JOB NAME', 'OWNER', 'JOB CLASS', 'JOB ID', 'RUN TIME', 'CPU', 'STEPS IN JCL',
                           'UTILITY/STEP NAME', 'COND CODES', 'RUN STATUS', 'REASON FOR ABEND', 'MORE ABEND INFO',
                           'STEP WISE DATA SETS', 'STARTED_TIME', 'ENDED_TIME', 'STEP AND DBRM LIBS', 'JOB INFO']]
            dataf.to_csv(r'C:\Users\m1055990\Desktop\ABEND.csv', index=False, mode='a', header=False)

        self.key_result.clear()
        self.total_steps.clear()
        self.Utility_result.clear()
        self.condcode_result.clear()
        self.final_result.clear()
        self.abend_result.clear()
        self.reason_list.clear()
        self.Lib_result.clear()
        self.temp_lib_res.clear()
        self.more_abend_info.clear()
        self.temp_ab_res.clear()
        self.job_info.clear()
        self.temp_job_info.clear()
