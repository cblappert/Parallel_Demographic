
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PopulationQuery {
	// next four constants are relevant to parsing
	public static final int TOKENS_PER_LINE  = 7;
	public static final int POPULATION_INDEX = 4; // zero-based indices
	public static final int LATITUDE_INDEX   = 5;
	public static final int LONGITUDE_INDEX  = 6;
	
	// parse the input file into a large array held in a CensusData object
	public static CensusData parse(String filename) {
		CensusData result = new CensusData();
		
        try {
            BufferedReader fileIn = new BufferedReader(new FileReader(filename));
            
            // Skip the first line of the file
            // After that each line has 7 comma-separated numbers (see constants above)
            // We want to skip the first 4, the 5th is the population (an int)
            // and the 6th and 7th are latitude and longitude (floats)
            // If the population is 0, then the line has latitude and longitude of +.,-.
            // which cannot be parsed as floats, so that's a special case
            //   (we could fix this, but noisy data is a fact of life, more fun
            //    to process the real data as provided by the government)
            
            String oneLine = fileIn.readLine(); // skip the first line

            // read each subsequent line and add relevant data to a big array
            while ((oneLine = fileIn.readLine()) != null) {
                String[] tokens = oneLine.split(",");
                if(tokens.length != TOKENS_PER_LINE)
                	throw new NumberFormatException();
                int population = Integer.parseInt(tokens[POPULATION_INDEX]);
                if(population != 0)
                	result.add(population,
                			   Float.parseFloat(tokens[LATITUDE_INDEX]),
                		       Float.parseFloat(tokens[LONGITUDE_INDEX]));
            }

            fileIn.close();
        } catch(IOException ioe) {
            System.err.println("Error opening/reading/writing input or output file.");
            System.exit(1);
        } catch(NumberFormatException nfe) {
            System.err.println(nfe.toString());
            System.err.println("Error in file format");
            System.exit(1);
        }
        return result;
	}

	// argument 1: file name for input data: pass this to parse
	// argument 2: number of x-dimension buckets
	// argument 3: number of y-dimension buckets
	// argument 4: -v1, -v2, -v3, -v4, or -v5
	public static void main(String[] args) {
		String filename = args[0];
		int xDim = Integer.parseInt(args[1]);
		int yDim = Integer.parseInt(args[2]);
		int version = Integer.parseInt(args[3].substring(2));
		// FOR YOU
	}

	public static void preprocess(String filename, int columns, int rows,
			int versionNum) {
		switch(versionNum) {
		case 1: preprocessOne(filename, columns, rows);
		case 2: preprocessTwo(filename, columns, rows);
		}
		
	}

	private static void preprocessTwo(String filename, int columns, int rows) {
		CensusData initial = parse(filename);
		float highLat = initial.data[0].latitude,
				lowLat = initial.data[0].latitude;
		float highLon = initial.data[0].longitude,
				lowLon = initial.data[0].longitude;
		for(int i = 0; i < initial.data_size; i++) {
			CensusGroup group = initial.data[i];
			highLat = Math.max(group.latitude, highLat);
			lowLat = Math.min(lowLat, group.latitude);
			highLon = Math.max(highLon, group.longitude);
			lowLon = Math.min(lowLon, group.longitude);
		}
		
	}

	private static void preprocessOne(String filename, int columns, int rows) {
		// TODO Auto-generated method stub
		
	}

	public static Pair<Integer, Float> singleInteraction(int w, int s, int e,
			int n) {
		// TODO Auto-generated method stub
		return null;
	}
}
