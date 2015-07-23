#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import redis
import time, datetime
import json

from conf import *


r_server = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


# Счетчик итервалов
counter = 0
# Время старта, секунды
start_timestamp = int(time.time())
# Время старта, datetime
start_time = datetime.datetime.now()


def log_monitor():
    global counter
    next_time = None

    while start_time + RUNNING_TIME > datetime.datetime.now():
        s_time = next_time or start_time if counter > 0 else None
        # обрабатываем текущую итерацию лога
        process_log(read_log(s_time))
        counter += 1
        # Засыпаем на заданный интервал
        time.sleep(INTERVAL)
        next_time = datetime.datetime.now()

    send_message()


def send_message():
    """
    Отправка сообщения
    """
    import smtplib
    from email.mime.text import MIMEText

    print get_message()

    msg = MIMEText(get_message().encode('utf-8'))
    msg['Subject'] = u'Отчет'
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL

    try:
        s = smtplib.SMTP('localhost')
        s.sendmail(EMAIL_FROM, [EMAIL], msg.as_string())
        s.quit()
    except Exception:
        pass


def get_message():
    """
    Возвращает сформированное сообщение об активности
    @return: {str} сформированное сообщение
    """
    uniq_ips = set()
    code_500 = 0
    code_404 = 0
    code_200 = 0
    count = 0
    message = u'Отчет об активности с %s по %s:\n' % (start_time, start_time + RUNNING_TIME)

    for result in get_results():
        uniq_ips = uniq_ips | set(result['ips'])
        code_500 += result['code_500']
        code_404 += result['code_404']
        code_200 += result['code_200']
        count += result['count']

    uniq_count = len(uniq_ips)
    uniq_ips = ', '.join(list(uniq_ips))
    message += u"""
    Всего переходов: %(count)s
    Уникальных переходов: %(uniq_count)s
    Уникальные ip: %(uniq_ips)s
    Кодов 500: %(code_500)s
    Кодов 404: %(code_404)s
    Кодов 200: %(code_200)s
    """ % locals()

    return message


def get_results():
    """
    Возвращает результаты из Redis
    @return: {list} результатов за интервалы
    """
    results = []
    for i in xrange(counter):
        log = r_server.get('%s:%s' % (start_timestamp, i))
        if log:
            results.append(json.loads(log))

    return results


def read_log(s_time=None):
    """
    Делаю генератором на случай если лог будет большим
    @param s_time: время с которого нужно прочитать лог
    @type s_time: {datetime}
    @return: объект "генератор"
    """
    regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    with open(LOG_PATH, 'r') as f:
        while True:
            raw_line = f.readline()
            if not raw_line:
                break
            line = re.match(regex, raw_line).groups()

            if s_time:
                # Костыль, так как в python 2 баг с определнием %z
                dt = datetime.datetime.strptime(line[1][0:20],
                                                '%d/%b/%Y:%H:%M:%S') +\
                    datetime.timedelta(hours=5)
                if dt >= s_time:
                    yield line
            else:
                yield line


def process_log(data):
    """
    Обработка лога
    @param data: данные лога за период
    @type data: {list}, <generator>
    """
    uniq_ips = set()
    code_500 = 0
    code_404 = 0
    code_200 = 0
    count = None

    for count, log in enumerate(data):
        uniq_ips.add(log[0])
        if log[3] == '500':
            code_500 += 1
        elif log[3] == '404':
            code_404 += 1
        elif log[3] == '200':
            code_200 += 1

    if not count is None:
        result = {
            'count': count + 1,
            'uniq_count': len(uniq_ips),
            'ips': list(uniq_ips),
            'code_200': code_200,
            'code_404': code_404,
            'code_500': code_500
        }

        r_server.set('%s:%s' % (start_timestamp, counter), json.dumps(result))


if __name__ == "__main__":
    log_monitor()
