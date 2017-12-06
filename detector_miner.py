from pattern_miner import PatternChunk, MiningPattern, Miner
import sys


DATETIME="(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})\s+(?P<time>\d+:\d+:\d+,\d+)"
LVLPID="\s+(?P<level>[A-Z]+):\s+PID:\s+(?P<pid>\d+)\s+\|\s+"
MESSAGE="(?P<message>[^\[]+)\[in\s+(?P<path>[^\:]+):(?P<line_number>\d+)]"

detector_pattern = MiningPattern([
    PatternChunk(DATETIME, is_repeating=False, optional=False),
    PatternChunk(LVLPID, is_repeating=False, optional=False),
    PatternChunk(MESSAGE, is_repeating=False, optional=False)
])
detector_miner = Miner(pattern=detector_pattern)

if __name__ == '__main__':
    pass

    
