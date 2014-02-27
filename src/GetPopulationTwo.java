import java.util.concurrent.RecursiveTask;

/* 
 * Helper class used to parallelize the querying of the CensusData. Returns
 * an integer representing the total population in the specified grid squares,
 * inclusive of the north, south, east, west grid squares. Ties are broken by
 * assigning CensusGroups to the North, East or Northeast groups. Edges on the
 * northernmost and easternmost sides are included in their respective grid squares. 
 */

public class GetPopulationTwo extends RecursiveTask<Integer> {
	private static final int SEQUENTIAL_CUTOFF = 1000;
	private CensusData cenData;
	private int rows, columns;
	private PreprocessResult preData;
	private int w, s, e, n, start, end;

	// Pre: CensusData passed in has at least one element in it [TODO FIND OUT IF THIS IS A VALID ASSUMPTION]
	//      grid coordinates within the grid, can be equal to maximum row/column number, otherwise throws
	// 		IllegalArgumentException()
	// Post: Constructs a new GetPopulationTwo instance with the specified parameters 
	public GetPopulationTwo(CensusData cenData, int rows, int columns, PreprocessResult preData, int w, int s, int e, int n, int start, int end) {
		if(w < 0 || s < 0 || n > rows + 1 || e > columns + 1) {
			throw new IllegalArgumentException();
		}
		this.cenData = cenData;
		this.rows = rows;
		this.columns = columns;
		this.preData = preData;
		this.w = w;
		this.s = s;
		this.e = e;
		this.n = n;
		this.start = start;
		this.end = end;
	}
	
	// Pre: N/A taken care of in constructor
	// Post: Computes the population contained in the grid squares specified in the constructor for the section
	//       of CensusData specified by start (inclusive) and end (exclusive). Calculates population inclusive
	//       of grid squares from the edges. Inclusive of the west and south edges of each grid square, and
	//       edges that border the north and east edges of the map. 
	@Override
	protected Integer compute() {
		if(end - start <= SEQUENTIAL_CUTOFF) {
			float latGridSize = Math.abs((preData.highLat - preData.lowLat) / rows);
			float lonGridSize = Math.abs((preData.highLon - preData.lowLon) / columns);
			float minLat = preData.lowLat + latGridSize * (s - 1);
			float maxLat = preData.lowLat + latGridSize * n;
			float minLon = preData.lowLon + lonGridSize * (w - 1);
			float maxLon = preData.lowLon + lonGridSize * e;
			if(maxLat == preData.highLat) maxLat++;
			if(maxLon == preData.highLon) maxLon++;
			// This increment is so that the points on the northernmost and easternmost edges will
			// be included in the places considered.
			int population = 0;
			for(int i = start; i < end; i++) {
				CensusGroup censusBlock = cenData.data[i];
				boolean isContained =  censusBlock.longitude >= minLon &&
						censusBlock.latitude >= minLat &&
						censusBlock.longitude < maxLon && 
						censusBlock.latitude < maxLat;
				if(isContained) {
					population += censusBlock.population;
				}
			}
			return population;
		} else {
			GetPopulationTwo right = new GetPopulationTwo(cenData, rows, columns, preData, w, s, e, n, (start + end) / 2, end);
			GetPopulationTwo left = new GetPopulationTwo(cenData, rows, columns, preData, w, s, e, n, start, (start + end) / 2);
			right.fork();
			int leftRes = left.compute();
			int rightRes = right.join();
			return leftRes + rightRes;
		}
	}

}
