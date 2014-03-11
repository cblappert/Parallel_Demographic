import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/*
 * Christopher Blappert and Michael Mitasev
 * 
 * Takes a CensusData object and a range and builds a grid corresponding to the
 * population of the grid squares. Uses fork-join parallelism to build the grid.
 */

public class ParallelBuildGrid extends RecursiveTask<int[][]> {
	
	private static final long serialVersionUID = -33805793052334406L;
	private int min;
	private int max;
	private static final int SEQUENTIAL_CUTOFF = 5000;
	private GridInfo ginfo;
	private CensusData cenData;
	private ForkJoinPool fjPool;
	
	// Pre: all parameters valid and not null
	// Post: creates a new instance of this object
	public ParallelBuildGrid(int min, int max, GridInfo info, CensusData data, ForkJoinPool fjPool) {
		this.min = min;
		this.max = max;
		ginfo = info;
		cenData = data;
		this.fjPool = fjPool;
	}
	
	// Post: Computes the populations of the grid by processing parts of the CensusData object in
	//       multiple fork-join threads.
	@Override 
	protected int[][] compute() {
		if (max -  min < SEQUENTIAL_CUTOFF) {
			int[][] populationGrid = new int[ginfo.getMaxCols()][ginfo.getMaxRows()];
			for (int i = 0; i < ginfo.getMaxCols(); i++) {
				for (int j = 0; j < ginfo.getMaxRows(); j++) {
					populationGrid[i][j] = 0;
				}
			}
			for (int i = min; i <= max; i++) {
				CensusGroup censusBlock = cenData.data[i];
				int x = (int) Math.floor((censusBlock.longitude - ginfo.getMinLon())/ginfo.getLonGridSize());
				int y = (int) Math.floor((censusBlock.latitude - ginfo.getMinLat())/ginfo.getLatGridSize());
				if (x == ginfo.getMaxCols()) {
					x--; //So the eastmost location gets added to the grid
				}
				if (y == ginfo.getMaxRows()) {
					y--; //So the northernmost location gets added to the grid
				}
				populationGrid[x][y] += censusBlock.population;
			}
			return populationGrid;
		} else {
			int mid = (min + max)/2;
			ParallelBuildGrid left = new ParallelBuildGrid(min, mid, ginfo, cenData, fjPool);
			ParallelBuildGrid right = new ParallelBuildGrid(mid + 1, max, ginfo, cenData, fjPool);
			left.fork();
			int[][] rightgrid = right.compute();
			int[][] leftgrid = left.join();
			// Because we don't know where the grid squares we read are, we have to sum both grids entirely. To reduce the
			// span we do this in parallel, so we fork extra threads to do this for us. 
			ParallelCombineGrid combine = new ParallelCombineGrid(leftgrid, rightgrid, 0, ginfo.getMaxCols() - 1, 0, ginfo.getMaxRows() - 1);
			fjPool.invoke(combine);
			return leftgrid;
		}
	}
}
