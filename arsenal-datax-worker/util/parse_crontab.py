# -*- coding: utf-8 -*-
# 用于解析crontab格式，判断是否该分钟是否需要运行
from datetime import datetime, timedelta

class ParseCrontab():

    def __init__(self, cron_expr):
        self.cron_expr=cron_expr
    
    # 匹配定时任务的单个区域,并返回运行的时间列表范围
    def parse_crontab_field(self, field, min_val, max_val, field_name):
        if field == '*':
            return list(range(min_val, max_val + 1))
        elif '*/' in field:
            step = int(field[2:])
            return list(range(min_val, max_val + 1, step))
        elif ',' in field:
            values = field.split(',')
            result = []
            for val in values:
                if val.isdigit():
                    parsed_val = int(val)
                    if parsed_val > max_val:
                        raise ValueError(f"Invalid {field_name} format")
                    result.append(parsed_val)
                elif '-' in val:
                    start, end = val.split('-')
                    parsed_start = int(start)
                    parsed_end = int(end)
                    if parsed_start > max_val or parsed_end > max_val:
                        raise ValueError(f"Invalid {field_name} format")
                    result.extend(range(parsed_start, parsed_end + 1))
            if len(result) == 0:
                raise ValueError(f"Invalid {field_name} format")
            return result
        elif '-' in field:
            start, end = field.split('-')
            result = list(range(int(start), int(end) + 1))
            if len(result) == 0:
                raise ValueError(f"Invalid {field_name} format")
            return result
        elif field.isdigit():
            val = int(field)
            if min_val <= val <= max_val:
                return [val]
        raise ValueError(f"Invalid {field_name} format")
 
    # 解析时间格式的范围
    def parse_crontab(self):
        if self.cron_expr:
            fields = self.cron_expr.split(' ')
            if len(fields) != 5:
                raise ValueError("Invalid crontab time format")
        else:
            raise ValueError("Crontab is null")

        try:
            minute = self.parse_crontab_field(fields[0], 0, 59, 'minute')
            hour = self.parse_crontab_field(fields[1], 0, 23, 'hour')
            day = self.parse_crontab_field(fields[2], 1, 31, 'day')
            month = self.parse_crontab_field(fields[3], 1, 12, 'month')
            weekday = self.parse_crontab_field(fields[4], 0, 6, 'weekday')

            return minute, hour, day, month, weekday
        except ValueError as e:
            raise ValueError(f"{e}")

    # 判断当前时间是否需要执行, 需要返回True, 不需要返回False, 也可用于判断定时任务格式是否正确传入dry_run非0参数即可
    def calculate_execution(self, dry_run=0):
        try:
            minute, hour, day, month, weekday = self.parse_crontab()
        except ValueError as e:
            print(f"定时任务[{self.cron_expr}]格式错误, 解析失败! error: [{e}]")
            return False

        if dry_run == 0:
            now = datetime.now()
            current_minute = now.minute
            current_hour = now.hour
            current_day = now.day
            current_month = now.month
            currnet_weekday = now.weekday()

            if current_minute in minute and current_hour in hour and current_day in day and current_month in month and currnet_weekday in weekday:
                print(f"分钟:{current_minute} 小时:{current_hour} 天:{current_day} 月:{current_month} 星期:{currnet_weekday} 符合[{self.cron_expr}]时间规则, 触发任务")
                return True
            else:
                print(f"分钟:{current_minute} 小时:{current_hour} 天:{current_day} 月:{current_month} 星期:{currnet_weekday} 不符合[{self.cron_expr}]时间规则, 不触发任务")
                return False
        else:
            print(f"定时任务[{self.cron_expr}]格式正确!")
            return True


if __name__ == "__main__":
    # 测试
    try:
        cron_expr = '* * * * * *'
        a=ParseCrontab(cron_expr)
        print(a.calculate_execution())
    except ValueError as e:
        print(e)
