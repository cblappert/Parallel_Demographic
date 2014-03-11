
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.concurrent.ForkJoinPool;

/*
 * Christopher Blappert and Michael Mitasev
 * 
 * This class allows the user to do queries about the population of the US in different areas.
 * It takes arguments corresponding to the input data about the US population, the x and y 
 * of the grid to be constructed, and the version used to process and query the data. The user can
 * then query the grid to find out the population of certain areas of the grid. Queries correspond
 * to 1-based indexes of the grid and are inclusive on both ends. (1, 1) is the southwest corner
 * of the grid.
 */

public class PopulationQuery {
	// next four constants are relevant to parsing
	public static final int TOKENS_PER_LINE = 7;
	public static final int POPULATION_INDEX = 4; // zero-based indices
	public static final int LATITUDE_INDEX = 5;
	public static final int LONGITUDE_INDEX = 6;
	
	// Public fields that correspond to data the program
	// stores each time it is run. These are necessary to 
	// allow singleInteraction to work with different versions
	public static ForkJoinPool fjPool;
	public static int instanceVersion;
	public static int gridColumns;
	public static int gridRows;
	public static PreprocessResult preData;
	public static CensusData cenData;
	public static int[][] populationGrid;

	// parse the input file into a large array held in a CensusData object
	public static CensusData parse(String filename) {
		CensusData result = new CensusData();

		try {
			@SuppressWarnings("resource")
			BufferedReader fileIn = new BufferedReader(new FileReader(filename));

			// Skip the first line of the file
			// After that each line has 7 comma-separated numbers (see constants above)
			// We want to skip the first 4, the 5th is the population (an int)
			// and the 6th and 7th are latitude and longitude (floats)
			// If the population is 0, then the line has latitude and longitude of +.,-.
			// which cannot be parsed as floats, so that's a special case
			// (we could fix this, but noisy data is a fact of life, more fun
			// to process the real data as provided by the government)

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
	/*
	 * Pre: arguments are valid
	 * Post: preprocesses the file according to the version, then prompts the user
	 *       for queries in the console, returning the population and % of total population
	 *       of the queried grid square. Throws IllegalArgumentException if the query
	 *       numbers are invalid. Exits if given any input that is not 4 integers separated
	 *       by spaces. 
	 */
	@SuppressWarnings("resource") // necessary for use in testing
	public static void main(String[] args) {
		String filename = args[0];
		// Initialize fields that are used throughout the program
		int columns = Integer.parseInt(args[1]);
		int rows = Integer.parseInt(args[2]);
		int version = Integer.parseInt(args[3].substring(2));
		
		// For testing purposes, we add an extra mode with files for input and output 
		boolean isTestMode = args.length == 6;
		Scanner console = new Scanner(System.in);
		PrintStream output = System.out;
		if(isTestMode) {
			try {
				console = new Scanner(new File(args[4]));
				output = new PrintStream(new File(args[5]));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		// Preprocess the file's data into easily queryable data
		// uses the same preprocessing method as the GUI for convenience
		preprocess(filename, columns, rows, version);
		
		// Process user input
		boolean hasQuery = true;
		while(hasQuery) {
			output.println("Please give west, south, east, north coordinates of your query rectangle:");
			int[] coords = new int[4];
			String input = "";
			if(console.hasNextLine()) {
				// Scan the line the user entered for the coordinates
				input = console.nextLine();
				Scanner lineScan = new Scanner(input);
				int i = 0;
				while(lineScan.hasNextInt() && i < coords.length) {
					coords[i] = lineScan.nextInt();
					i++;
				}
				hasQuery = (i == coords.length) && !lineScan.hasNext();
				// means their input was 4 integers 
				if(hasQuery) { 
					Pair<Integer, Float> queryAnswer = singleInteraction(coords[0], coords[1], coords[2], coords[3]);
					output.println("population of rectangle: " + queryAnswer.getElementA());
					output.println("percent of total population: " + queryAnswer.getElementB());
				}
				lineScan.close();
			} else {
				hasQuery = false;
			}	
		}
		console.close();
	}

	// Pre: filename valid, columns and rows nonnegative, version between 1 and 5.
	// Post: Processes the input from the specified file, using the specified number of rows and columns,
	//       and the specified version. 
	public static void preprocess(String filename, int columns, int rows, int version) {
		// These are set to fields because they are necessary for keeping this data between preprocess and query time.
		gridColumns = columns;
		gridRows = rows;
		instanceVersion = version;
		fjPool = new ForkJoinPool();
		switch(version) {
		case 1: preData = findCornersPopSeq(filename); break;
		case 2: preData = findCornersPopPara(filename); break;
		case 3: Pair<PreprocessResult, int[][]> tempPreResThree = preprocessGridSeq(filename);
		preData = tempPreResThree.getElementA();
		populationGrid = tempPreResThree.getElementB(); break;
		case 4: Pair<PreprocessResult, int[][]> tempPreResFour = preprocessGridPara(filename);
		preData = tempPreResFour.getElementA();
		populationGrid = tempPreResFour.getElementB(); break;
		case 5: Pair<PreprocessResult, int[][]> tempPreResFive = preprocessGridLock(filename); 
		preData = tempPreResFive.getElementA();
		populationGrid = tempPreResFive.getElementB(); break;
		}
	}

	// Pre: coordinates entered valid, else throws IllegalArgumentException
	// Post: Does a single query for the population of the specified rectangle of grid squares. Works
	//       based on the version being used by the rest of the program.
	public static Pair<Integer, Float> singleInteraction(int w, int s, int e,
			int n) {
		if(w < 1 || s < 1 || e > gridColumns || n > gridRows || 
				e < w || n < s) { // coordinate was invalid
			throw new IllegalArgumentException();
		}
		switch(instanceVersion) {
		case 1: return getPopulationSimpleSequential(preData, w, s, e, n);
		case 2: return getPopulationSimplePara(preData, w, s, e, n);
		case 3: case 4: case 5: return getPopulationFromGrid(preData, w, s, e, n);
		}
		return null;
	}

	// Pre: filename is valid, file in readable location
	// Post: simple sequential preprocessing [version 1], returns the 
	// 		 total population and the borders of the map.
	private static PreprocessResult findCornersPopSeq(String filename) {
		cenData = parse(filename);
		PreprocessResult result = new PreprocessResult();
		result.highLat = cenData.data[0].latitude;
		result.lowLat = result.highLat;
		result.highLon = cenData.data[0].longitude;
		result.lowLon = result.highLon;
		result.totPop = 0;
		for (int i = 0; i < cenData.data_size; i++) {
			CensusGroup group = cenData.data[i];
			result.totPop += group.population;
			result.highLat = Math.max(result.highLat, group.latitude);
			result.lowLat = Math.min(result.lowLat, group.latitude);
			result.highLon = Math.max(result.highLon, group.longitude);
			result.lowLon = Math.min(result.lowLon, group.longitude);
		}
		return result;
	}
		
	// Pre: filename is valid, file in readable location
	// Post: simple parallel preprocessing [version 2], uses fork-join parallelism
	//       returns the total population and the borders of the map.
	private static PreprocessResult findCornersPopPara(String filename) {
		cenData = parse(filename);
		FindCornersPopParalell process = new FindCornersPopParalell(cenData, 0, cenData.data_size);
		PreprocessResult result = fjPool.invoke(process);
		return result;
	}

	// Pre: filename is valid, file is in readable location
	// Post: grid-based sequential preprocessing [verison 3] 
	// 		 returns the corners of the map, the total population
	// 		 and the grid created that makes queries efficient
	private static Pair<PreprocessResult, int[][]> preprocessGridSeq(String filename) {
		PreprocessResult preData = findCornersPopPara(filename);
		float latGridSize = Math.abs((preData.highLat - preData.lowLat) / gridRows);
		float lonGridSize = Math.abs((preData.highLon - preData.lowLon) / gridColumns);
		int[][] populationGrid = new int[gridColumns][gridRows];
		for (int i = 0; i < cenData.data_size; i++) {
			CensusGroup censusBlock = cenData.data[i];
			int x = (int) Math.floor((censusBlock.longitude - preData.lowLon)/lonGridSize);
			int y = (int) Math.floor((censusBlock.latitude - preData.lowLat)/latGridSize);
			if (x == gridColumns) {
				x--; //So the eastmost location gets added to the grid
			}
			if (y == gridRows) {
				y--; //So the northernmost location gets added to the grid
			}
			populationGrid[x][y] += censusBlock.population;
		}

		updateGridToSum(populationGrid);
		return new Pair<PreprocessResult, int[][]>(preData, populationGrid);
	}
	
	// Pre: filename is valid, file is in readable location
	// Post: grid-based parallel preprocessing [verison 4] using fork-join
	//       parallelism. returns the corners of the map, the total population
	// 		 and the grid created that makes queries efficient
	private static Pair<PreprocessResult, int[][]> preprocessGridPara(String filename) {
		PreprocessResult preData = findCornersPopPara(filename);
		float latGridSize = Math.abs((preData.highLat - preData.lowLat) / gridRows);
		float lonGridSize = Math.abs((preData.highLon - preData.lowLon) / gridColumns);
		
		GridInfo ginfo = new GridInfo(gridColumns, gridRows, latGridSize, lonGridSize, preData.lowLat, preData.lowLon);
		ParallelBuildGrid processToGrid = new ParallelBuildGrid(0, cenData.data_size - 1, ginfo, cenData, fjPool);
		int [][] populationGrid = fjPool.invoke(processToGrid);

		updateGridToSum(populationGrid);
		
		return new Pair<PreprocessResult, int[][]>(preData, populationGrid);
	}
	
	// Helper method that converts the grid that contains populations corresponding
	// to their grid squares to the grid that contains the population of the rectangle
	// consisting of the north-west corner of the country to the lower right corner of the
	// grid square.
	private static void updateGridToSum(int[][] populationGrid) {
		for (int i = 1; i < gridColumns; i++) {
			populationGrid[i][0] += populationGrid[i-1][0]; 
		}
		for (int j = 1; j < gridRows; j++) {
			populationGrid[0][j] += populationGrid[0][j-1];
		}
		for (int i = 1; i < gridColumns; i++) {
			for (int j = 1; j < gridRows; j++) {
				populationGrid[i][j] += (populationGrid[i-1][j] + populationGrid[i][j-1] - populationGrid[i-1][j-1]);
			}
		}
	}

	// Pre: filename is valid, file is in readable location
	// Post: grid-based concurrent preprocessing [verison 5] using locks and multiple
	//       threads. returns the corners of the map, the total population
	// 		 and the grid created that makes queries efficient
	private static Pair<PreprocessResult, int[][]> preprocessGridLock(String filename) {
		PreprocessResult preData = findCornersPopPara(filename);
		float latGridSize = Math.abs((preData.highLat - preData.lowLat) / gridRows);
		float lonGridSize = Math.abs((preData.highLon - preData.lowLon) / gridColumns);
		GridInfo ginfo = new GridInfo(gridColumns, gridRows, latGridSize, lonGridSize, preData.lowLat, preData.lowLon);
		int[][] populationGrid = new int[gridColumns][gridRows];
		Object[][] locks = new Boolean[gridColumns][gridRows]; // boolean so that they dont take up much space
		for(int i = 0; i < locks.length; i++) {
			for(int j = 0; j < locks[0].length; j++) {
				locks[i][j] = false;
			}
		}
		PreprocessBuildGridLock preprocessor = new PreprocessBuildGridLock(0, cenData.data_size, ginfo, cenData, populationGrid, locks);
		populationGrid = preprocessor.calculatePopulationGrid();
		
		updateGridToSum(populationGrid);
		
		return new Pair<PreprocessResult, int[][]>(preData, populationGrid);
	}
	

	// Pre: query given is valid
	// Post: Sequentially calculates the population within a given range,
	//       and its % of the us population. [version 1 query]
	private static Pair<Integer, Float> getPopulationSimpleSequential(
			PreprocessResult preData, int w, int s, int e, int n) {
		float latGridSize = Math.abs((preData.highLat - preData.lowLat) / gridRows);
		float lonGridSize = Math.abs((preData.highLon - preData.lowLon) / gridColumns);
		float minLatitude = preData.lowLat + latGridSize * (s - 1); //-1 to make it inclusive
		float maxLatitude = preData.lowLat + latGridSize * n;
		float minLongitude = preData.lowLon + lonGridSize * (w - 1); //-1 to make it inclusive
		float maxLongitude = preData.lowLon + lonGridSize * e;
		if(maxLatitude == preData.highLat) { //To ensure top part of the rectangle is counted 
			maxLatitude++;
		}
		if(maxLongitude == preData.highLon) {//To ensure eastern part of the rectangle is counted
			maxLongitude++;
		}
		int population = 0;
		for(int i = 0; i < cenData.data_size; i++) {
			CensusGroup censusBlock = cenData.data[i];
			boolean isContained = (censusBlock.longitude >= minLongitude && censusBlock.latitude >= minLatitude &&
					censusBlock.longitude < maxLongitude && censusBlock.latitude < maxLatitude);
			if(isContained) {
				population += censusBlock.population;
			}
		}
		float percentPop = (float) (Math.round(100 * (float) (100.0 * population / preData.totPop)) / 100.0);
		return new Pair<Integer, Float>(population, percentPop);
	}

	// Pre: query given is valid
	// Post: calculates the population within a given range and its % of the us population
	//       using fork-join parallelism [version 2 query]
	private static Pair<Integer, Float> getPopulationSimplePara(PreprocessResult preData,
			int w, int s, int e, int n) {
		int population = -1;
		GetPopulationParallel process = new GetPopulationParallel(cenData, gridRows, gridColumns, preData, w, s, e, n, 0, cenData.data_size);
		population = fjPool.invoke(process);
		float percentPop = (float) (Math.round(100 * (float) (100.0 * population / preData.totPop)) / 100.0);
		return new Pair<Integer, Float>(population, percentPop);
	}
	
	// Pre: query given is valid
	// Post: calculates the population within a given range and its % of the us population
	//       using the pre-computed grid of sums [version 3, 4, 5 query]
	public static Pair<Integer, Float> getPopulationFromGrid(
			PreprocessResult preData4, int w, int s, int e, int n) {
		int population = populationGrid[e - 1][n - 1];
		boolean furtherWestSquare = (w > 1);
		boolean furtherSouthSquare = (s > 1);
		if (furtherWestSquare) {
			population -= populationGrid[w - 2][n - 1];
		}
		if (furtherSouthSquare) {
			population -= populationGrid[e - 1][s - 2];
		}
		if (furtherWestSquare && furtherSouthSquare) {
			population += populationGrid[w - 2][s - 2];
		}
		float percentPop = (float) (Math.round(100 * (float) (100.0 * population / preData4.totPop)) / 100.0);
		return new Pair<Integer, Float>(population, percentPop);
	}
}

