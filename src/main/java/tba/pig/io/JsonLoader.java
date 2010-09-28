package tba.pig.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.twitter.elephantbird.pig.util.PigCounterHelper;

// reuses code from Twitter's Elephant Bird LzoJsonLoader and 
// the 0.7.0 loader docs at http://hadoop.apache.org/pig/docs/r0.7.0/udf.html#Load+Functions
public class JsonLoader extends LoadFunc {
    private static final Logger LOG = Logger.getLogger(JsonLoader.class);

    private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();

    private final JSONParser jsonParser_ = new JSONParser();

    private final PigCounterHelper counterHelper_ = new PigCounterHelper();

    private RecordReader reader;

    private TextInputFormat inputFormat;

    protected static enum Counters {
        LinesRead,
        LinesJsonDecoded,
        LinesParseError,
        LinesParseErrorBadNumber
    }

    public JsonLoader() {
        LOG.info("LzoJsonLoader creation");
    }

    public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
        // Since we are not block aligned we throw away the first record of each split and count on a different
        // instance to read it. The only split this doesn't work for is the first.
        if (!atFirstRecord) {
            getNext();
        }
    }

    /**
     * Return every non-null line as a single-element tuple to Pig.
     */
    public Tuple getNext() throws IOException {
        try {
            boolean notDone = reader.nextKeyValue();
            if (!notDone) {
                return null;
            }
            String line = reader.getCurrentValue().toString();
            incrCounter(Counters.LinesRead, 1L);

            Tuple t = parseStringToTuple(line);
            if (t != null) {
                incrCounter(Counters.LinesJsonDecoded, 1L);
                return t;
            }

            return null;
        }
        catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    private void incrCounter(Counters counter, long amt) {
        counterHelper_.incrCounter(counter, amt);

    }

    protected Tuple parseStringToTuple(String line) {
        try {
            Map<String, String> values = new HashMap<String, String>();
            JSONObject jsonObj = (JSONObject) jsonParser_.parse(line);
            for (Object key : jsonObj.keySet()) {
                Object value = jsonObj.get(key);
                values.put(key.toString(), value != null ? value.toString() : null);
            }
            return tupleFactory_.newTuple(values);
        }
        catch (ParseException e) {
            LOG.warn("Could not json-decode string: " + line, e);
            incrCounter(Counters.LinesParseError, 1L);
            return null;
        }
        catch (NumberFormatException e) {
            LOG.warn("Very big number exceeds the scale of long: " + line, e);
            incrCounter(Counters.LinesParseErrorBadNumber, 1L);
            return null;
        }
        catch (ClassCastException e) {
            LOG.warn("Could not convert to Json Object: " + line, e);
            incrCounter(Counters.LinesParseError, 1L);
            return null;
        }
    }

    @Override    
    public InputFormat getInputFormat() throws IOException {
        if (inputFormat == null) {
            inputFormat = new TextInputFormat();
        }
        return inputFormat;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        TextInputFormat.setInputPaths(job, location);
    }

}
