from datetime import datetime
from functools import wraps, partial
import logging
import pandas as pd
import re
import sys


# Have a verbose setting for when you have to go FULL MARXIST HISTORIAN
logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs:\
                         inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs:\
                  logging.log(logging.VERBOSE, msg, *args, **kwargs)  

_logging_vals = dict(
    format=None, 
    level=logging.VERBOSE, 
    filename="pattern_miner.log",
    filemode='w'
)


if not _logging_vals.get("format"):
    _logging_vals["format"] = '%(asctime)s - %(name)s - '+\
                 '%(levelname)s - %(message)s'
    logging.basicConfig(**_logging_vals)
    logger = logging.getLogger(__name__)

def updt(total, progress):
    """
    Displays or updates a console progress bar.

    Original source: https://stackoverflow.com/a/15860757/1391441
    This version is from stack overflow user Gabriel:
        https://stackoverflow.com/users/1391441/gabriel
    """
    barLength, status = 20, ""
    progress = float(progress) / float(total)
    if progress >= 1.:
        progress, status = 1, "\r\n"
    block = int(round(barLength * progress))
    text = "\r[{}] {:.2f}% {}".format(
        "#" * block + "-" * (barLength - block), round(progress * 100, 2),
        status)
    sys.stdout.write(text)
    sys.stdout.flush()


_CCFNS = dict(
    CONVERT_DATE="convert_date",
    CONVERT_TIME="convert_time",
    CONVERT_DATETIME="convert_datetime",
    MERGE_DATE_AND_TIME="merge_date_and_time"
)

class ChunkCleaner(object):
    def __init__(self, gem_clean_map, **kwargs):
        self.gems = {}
        for gem, op in gem_clean_map.items():
            gemargs = kwargs.get(gem)
            if gemargs:
                self.gems = partial(getattr(self, _CCFNS[op]), **gemargs)  
            else:
                self.gems[gem] = getattr(self, _CCFNS[op])

    def __iter__(self):
        for gem, op in self.gems.items():
            yield(gem, op)
            
    @staticmethod
    def convert_date(date, frmt="%Y-%m-%d"):
        return datetime.strptime(date, frmt)

    @staticmethod
    def convert_time(_time, frmt="%H:%M:%S,%f"): 
        return datetime.strptime(_time, frmt)

    @staticmethod
    def convert_datetime(dtime, frmt="%Y-%m-%d %H:%M:%S,%f"):
        return datetime.strptime(dtime, frmt)


class PatternChunk(object):
    def __init__(self, pattern, is_repeating, optional, cleaning_ops=None):
        self.repeats = is_repeating
        self.optional = optional
        self.pattern = re.compile(pattern) if type(pattern) == str else pattern
        self.gems = dict()
        self._prep_chunk(cleaning_ops)

    def _prep_chunk(self, cleaning_ops=None):
        gcol = re.compile("\?P\<([\w\d]+)", re.MULTILINE)
        to_id = gcol.findall(self.pattern.pattern)
        if to_id:
            if self.repeats:
                to_id = [tid + "_#" for tid in to_id]
                logger.info(
                    f"MiningPattern is looking for repeating values: '{to_id}'"
                )
            else:
                logger.info(f"MiningPattern is looking for: '{to_id}'")
            self.gems.update({tid: None for tid in to_id})
            logger.debug(
                f"Preparing cleaning operations for chunk: '{self}'"
            )
            if cleaning_ops:
                for gem, op in cleaning_ops:
                    if gem not in self.gems:
                        raise IndexError(f"No gem named '{gem}' in chunk")
                    else:
                        self.gems[gem] = op
        else:
            logger.warning(
                f"Could not find anything to look for in chunk: '{self}'"
            )

    def match(self, line):
        m = self.pattern.match(line)
        if m:
            groupdict = m.groupdict()
            for k in groupdict.keys():
                op = self.gems.get(k)
                if op:
                    logger.verbose(f"Cleaning '{k}' with '{op}'")
                    groupdict[k] = op(groupdict[k])                
            return groupdict, line[m.span()[1]:]
        else:
            return None, line

    def __repr__(self):
        rep = f"<PatternChunk {repr(self.pattern.pattern)}"
        if self.optional:
            rep += " OPTIONAL "
        if self.repeats:
            rep += " REPEATS "
        return rep.strip() + ">"

_PCFNS = dict(
    MERGE_DATE_AND_TIME="merge_date_and_time"
)

class PatternCleaner(object):
    def __init__(self, clean_ops):
        self.operations = []
        for op, gemargs in clean_ops:
            if gemargs:
                self.operations.append(partial(
                    getattr(self, _PCFNS[op]), **gemargs
                ))  
            else:
                self.operations.append(getattr(self, _PCFNS[op]))

    @staticmethod
    def merge_date_and_time(presults, datecol="date",
                            timecol="time", outcol="datetime"):
        date = presults[datecol]
        _time = presults[timecol]
        presults[outcol] = datetime.combine(date, _time.time())  
        del presults[datecol]
        del presults[timecol]
        return presults

    def clean(self, presults):
        operations = self.operations.copy()
        while operations:
            operation = operations.pop(0)
            presults = operation(presults)
        return presults

class MiningPattern(object):
    def __init__(self, chunks, post_clean_ops=None):
        self.chunks = chunks
        self.pattern_begun = False
        self.errors = []
        self.skipped = []
        self.gems = []
        self._prep_pattern()
        logger.debug(f"Setting '_clean_ops' to {post_clean_ops}")
        self._clean_ops = post_clean_ops

    def _prep_pattern(self, post_clean_ops=None):
        for chunk in self.chunks:
            gems = chunk.gems.keys()           
            self.gems.extend(gems)

        logger.debug(
            f"Gems pattern '{self}' is looking for are: '{self.gems}'"
        )

    def _match_chunk(self, start, chunk, line, i, in_prog=0):
        presult = None

        logger.debug(f"On chunk: '{chunk}'")
        m, line = chunk.match(line)
        if m:
            if not self.pattern_begun:
                logger.verbose("Pattern started")
                self.pattern_begun = True
            logger.verbose(f"Matched chunk '{chunk}' on {line}")
            if chunk.repeats:
                d = dict()
                for k,v in m.items():
                    d[k+f"_{in_prog}"] = v
                return self._return_result(d, start, i), line
            else:
                return self._return_result(m, start, i), line

        elif chunk.optional and self.pattern_begun:
            logger.verbose(f"Incremening chunk_id to '{chunk_id}'")
            if len(self.chunks) == chunk_id:
                logger.verbose(f"No chunks left, returning {presult}")
                return presult, line
        elif not self.pattern_begun:
            msg = "Pattern has not begun, and no match."
            logger.verbose(msg + f" Returning {presult}.")
            return presult, line
        elif chunk.repeats:
            logger.verbose(f"Chunk {chunk} at end.")
            return None, line
        else:
            return False, line

    def _return_result(self, md, start, i):
        presult = md.copy()
        if start == None:
            presult["start"] = i
        presult["end"] = i
        logger.verbose(f"Returning result '{presult}'")
        return presult

    def _cascade_chunks(self, line):
        #TODO: Figure this shit out
        chunks = self.chunks[1:]
        while chunks:
            chunk = chunks.pop(0)
            logger.verbose(f"Trying chunk: {chunk}")
            if chunk.match(line):
                self.chunks = chunks
                return self._return_result(chunk.match(line).groupdict())
        return None
    
    def _reset_pattern(self, presults):
        self.pattern_begun = False
        if self._clean_ops:
            logger.debug(
                f"Running pattern cleaner '{self._clean_ops}' on '{presults}'"
            )
            return self._clean_ops.clean(presults)
        else:
            return presults

    def match(self, lines, start=None):
        presults = dict(start=start, end=0)
        in_prog = 0
        chunks = self.chunks.copy()
        while chunks and lines:
            i, line = lines.pop(0)
            logger.verbose(f"Popped line #{i}: {repr(line)}")
            while line:
                logger.verbose(f"Running on line {repr(line)}...")
                if len(chunks) == 0:
                    logger.verbose(
                        f"Out of chunks...breaking from {repr(line)}"
                    )
                    break
                else:
                    chunk = chunks[0]
                    logger.verbose("...with chunk: {repr(chunk)}")
                presult, line = self._match_chunk(
                    presults.get("start"), chunk, line, i, in_prog
                )
                if presult:
                    if chunks[0].repeats:
                        logger.verbose(
                            f"Setting in_prog from {in_prog} to {in_prog+1}"
                        ) 
                        in_prog += 1
                    else:
                        logger.verbose(f"Popping chunk: {chunks[0]}")
                        chunks.pop(0)
                    presults.update(presult)
                    presults["end"] += 1
                elif presult == False:
                    logger.debug(
                        f"Could not match {line} with {chunk}"
                    )
                    self.errors.append((line, chunk))
                    return False
                else:
                    if chunks[0].repeats:
                        in_prog = 0
                        msg = "Encountered end of repeating chunk. "
                        logger.verbose(msg+f"Popping chunk: {chunks[0]}")
                        chunks.pop(0)
                        break
                    elif not self.pattern_begun:
                        logger.verbose(
                            "Pattern has not begun, so skipping line."
                        )
                        break
                    else:
                        logger.warning("Unexpected condition in match()")            
        
        if chunks and not lines and self.pattern_begun:
            raise IndexError(
                f"There are '{len(self.chunks)}' chunks and no lines left."
            )
        else:
            logger.debug("Pattern matched. Reseting pattern")
            return self._reset_pattern(presults)


class Miner(object):
    def __init__(self, pattern):
        self._current_doc = []
        self.mined_store = dict(
            _meta=dict(
                records=0,
                indices=[]
            )
        )
        self.pattern = pattern

    def __len__(self):
        return self.mined_store['_meta']['records']

    def _prep_doc(self, fname):
        if type(fname) == list:
            last = 0
            self._current_doc = []
            while fname:
                curdoc = fname.pop(0)
                logger.info(f"Loading '{curdoc}'...")                      
                with open(curdoc, 'r', errors="replace") as f:
                    self._current_doc.extend(
                        (i+last,l) for i,l in enumerate(f.readlines())
                    )
                    last = len(self._current_doc)
                logger.info(f"Loaded {fname} with '{last}' records.")
        else:
            with open(fname, 'r', errors="replace") as f:
                self._current_doc = [(i,l) for i,l in enumerate(f.readlines())]

    def modify_mined_store(method):
        @wraps(method)
        def _impl(self, *args, **kwargs):
            di = kwargs.get('doc_index', "")
            res = None
            if not di:
                logger.debug(
                    f"No 'doc_index' key in kwargs; kwargs='{kwargs}'"
                )
                # TODO: Note that 'doc_index' is expected to be first arg
                # if not in kwargs
                di = args[0]
                if not di:
                    logger.warning("Couldn't find index in args or kwargs")
            if di:
                logger.debug(f"Modifying index for '{di}'")
                self.mined_store['_meta']['indices'].append(di)
            try:
                res = method(self, *args, **kwargs)
            except:
                logger.exception("Uncaught exception")
                if di in self.mined_store['_meta']['indices']:
                    self.mined_store['_meta']['indices'].remove(di)
                raise
            self.mined_store['_meta']['records'] = sum([
                len(self.mined_store[k])\
                for k in self.mined_store['_meta']['indices']\
                if k in self.mined_store
            ])
            return res
        return _impl        

    @modify_mined_store
    def mine_document(self, doc_index, fname, log_type=None):
        self._prep_doc(fname)
        if not doc_index in self.mined_store:
            self.mined_store[doc_index] = []
        lines = self._current_doc
        total = len(lines)
        start = None
        ind = None
        line = None
        while lines:           
            pres = self.pattern.match(lines, start)
            logger.debug(
                f"Appending result '{pres}' to mined_store '{doc_index}'"
            )
            if pres:
                self.mined_store[doc_index].append(pres)
                logger.debug(
                    f"Moving lines from index '{start}' to "
                    f"'{pres.get('end') + 1}'"
                )
                start = pres.get("end") + 1
                updt(total, start)

    @modify_mined_store
    def offload_document_into_dataframe(self, doc_index, popdata=True):
        df = pd.DataFrame(self.mined_store[doc_index])
        if popdata:
            logger.info(f"Removing index '{doc_index}'")
            del self.mined_store[doc_index]
            self.mined_store['_meta']['indices'].remove(doc_index)
            logger.info(
                f"Remaining indices are {self.mined_store['_meta']['indices']}"
            )
        return df

    def mine_document_into_dataframe(self, doc_index, fname, popdata=True):
        self.mine_document(doc_index, fname)
        return self.offload_document_into_dataframe(doc_index, popdata)

