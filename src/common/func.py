# contains helper functions

# In stop_times.txt file, there exist a lot of times
# These times need to be converted to unix format for easy comparison
def unix_convert(val):
    if val == 'arrival_time':
        return
    splitVal = val.split(':')
    return int(splitVal[0])*60*60 + int(splitVal[1])*60 + int(splitVal[2])

# Checks if the value is indeed an integer enclosed in string quotes
# Required because of some dumb error that spark throws for unknown reason
# [occurs when you remove the first row from the data but it doesn't get picked up for w.e reason]
def representsInt(s):
    try:
        int(s)
        return s
    except ValueError:
        return None
