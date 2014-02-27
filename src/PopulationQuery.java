
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ForkJoinPool;

public class PopulationQuery {
	// next four constants are relevant to parsing
	public static final int TOKENS_PER_LINE  = 7;
	public static final int POPULATION_INDEX = 4; // zero-based indices
	public static final int LATITUDE_INDEX   = 5;
	public static final int LONGITUDE_INDEX  = 6;
	public static ForkJoinPool fjPool = new ForkJoinPool();
	public static int version;
	public static int columns;
	public static int rows;
	public static PreprocessResult preData;
	public static CensusData cenData;
	
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
		// TODO CHANGE TO actual command line input ?
		columns = Integer.parseInt(args[1]);
		rows = Integer.parseInt(args[2]);
		version = Integer.parseInt(args[3].substring(2));
		preData = preprocess(filename, columns, rows, version);
		
		Scanner console = new Scanner(System.in);
		boolean hasQuery = true;
		while(hasQuery) {
			System.out.println("Please give west, south, east, north coordinates of your query rectangle:");
			int w = 0, s = 0, e = 0, n = 0;
			String input = "";
			if(console.hasNextLine()) {
				input = console.nextLine();
				Scanner lineScan = new Scanner(input);
				if(lineScan.hasNextInt()) {
					w = lineScan.nextInt();
				} else {
					hasQuery = false;
				}
				if(lineScan.hasNextInt()) {
					s = lineScan.nextInt();
				} else {
					hasQuery = false;
				}
				if(lineScan.hasNextInt()) {
					e = lineScan.nextInt();
				} else {
					hasQuery = false;
				}
				if(lineScan.hasNextInt()) {
					n = lineScan.nextInt();
				} else {
					hasQuery = false;
				}
				if(hasQuery) {
					Pair<Integer, Float> queryAnswer = singleInteraction(w, s, e, n);
					System.out.println("population of rectangle: " + queryAnswer.getElementA());
					System.out.println("percent of total population: " + queryAnswer.getElementB());
				}
			} else {
				hasQuery = false;
			}
			
		}
	}

	// Processes the input from the specified file, using the specified number of rows and columns,
	// and the specified version. Returns a PreprocessResult object which contains the maximum and minimum
	// latitudes and longitudes as well as the total population. 
	public static PreprocessResult preprocess(String filename, int columns, int rows, int version) {
		PreprocessResult res = null;
		switch(version) {
		case 1: res = preprocessOne(filename);
		case 2: res = preprocessTwo(filename);
		}
		return res;
	}

	// Wrapper method that invokes the multithreaded version 2 preprocessing. Also parses the file
	// into the cenData field.
	private static PreprocessResult preprocessTwo(String filename) {
		cenData = parse(filename);
		PreprocessTwo process = new PreprocessTwo(cenData, 0, cenData.data_size);
		PreprocessResult result = fjPool.invoke(process);
		return result;
	}
	
	// Wrapper method that invokes the multithreaded query for the population in the specified grid
	// rectangle. Returns a pair of the population as a raw number and the population as a percentage
	// of the total. 
	private static Pair<Integer, Float> getPopulationTwo(PreprocessResult preData, 
			int w, int s, int e, int n) {
		int population = -1;
		GetPopulationTwo process = new GetPopulationTwo(cenData, rows, columns, preData, w, s, e, n, 0, cenData.data_size);
		population = fjPool.invoke(process);
		float percentPop = (float) (Math.round(100 * (float) (100.0 * population / preData.totPop)) / 100.0);
		return new Pair<Integer, Float>(population, percentPop);
	}

	// MICHAEL PUT YOUR WRAPPER HERE
	private static PreprocessResult preprocessOne(String filename) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// MICHAEL PUT YOUR OTHER WRAPPER HERE
	private static Pair<Integer, Float> getPopulationOne(
			PreprocessResult preData2, int w, int s, int e, int n) {
		// TODO Auto-generated method stub
		return null;
	}

	// Does a single query for the population of the specified rectangle of grid squares. Works
	// based on the version being used by the rest of the program. 
	public static Pair<Integer, Float> singleInteraction(int w, int s, int e,
			int n) {
		switch(version) {
		case 1: return getPopulationOne(preData, w, s, e, n);
		case 2: return getPopulationTwo(preData, w, s, e, n);
		}
		return null;
	}
}
