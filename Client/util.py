# #the func has one parameter: sub range of data
def do_in_sub_range(step, data, func):
    start = 0
    while start < len(data):
        stop_tmp = start + step
        stop = stop_tmp if stop_tmp < len(data) else len(data)
        sub_data = data[start: stop]
        func(sub_data)
        start = stop


