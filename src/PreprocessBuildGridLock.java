/*
 * Christopher Blappert and Michael Mitasev
 * 
 * Helper class to preprocess the census data into a grid object with each
 * grid square corresponding to the population contained in that grid square.
 * Uses locks to allow concurrency between the 4 threads.
 */

public class PreprocessBuildGridLock extends java.lang.Thread {
	private static final int NUM_THREADS = 4;
	private int min;
	private int max;
	private GridInfo ginfo;
	private CensusData cenData;
	private int[][] populationGrid;
	private Object[][] locks;
	
	// Pre: data not null, locks not null, populationGrid and locks have same dimension
	// Post: creates a new object. Processing is exclusive of max inclusive of min.
	public PreprocessBuildGridLock(int min, int max, GridInfo info, CensusData data, int[][] populationGrid, Object[][] locks) {
		this.min = min;
		this.max = max;
		ginfo = info;
		cenData = data;
		this.populationGrid = populationGrid;
		this.locks = locks;
	}
	
	// Post: runs the thread on the specified range of the CensusData object to calculate the population
	// 		 of the grid squares. 
	public void run() {
		for (int i = min; i < max; i++) {
			CensusGroup censusBlock = cenData.data[i];
			int x = (int) Math.floor((censusBlock.longitude - ginfo.getMinLon())/ginfo.getLonGridSize());
			int y = (int) Math.floor((censusBlock.latitude - ginfo.getMinLat())/ginfo.getLatGridSize());
			if (x == ginfo.getMaxCols()) {
				x--; //So the eastmost location gets added to the grid
			}
			if (y == ginfo.getMaxRows()) {
				y--; //So the northernmost location gets added to the grid
			}
			synchronized(locks[x][y]) { // only need to lock right before update
				populationGrid[x][y] += censusBlock.population;
			}
		}
	}
	
	// Post: Forks the initial threads that then process the CensusData object
	// 		 into the grid. Then returns the grid. 
	public int[][] calculatePopulationGrid() {
		PreprocessBuildGridLock[] threads = new PreprocessBuildGridLock[NUM_THREADS];
		int threadWork = (max + min) / 4;
		for(int i = 1; i < NUM_THREADS; i++) {
			threads[i - 1] = new PreprocessBuildGridLock(threadWork * (i - 1), threadWork * i, ginfo, cenData, populationGrid, locks);
		}
		// last has to be explicitly pulled out to make sure we go up to the max due to roundoff errors with integer division
		threads[NUM_THREADS - 1] = new PreprocessBuildGridLock(threadWork * (NUM_THREADS - 1), max, ginfo, cenData, populationGrid, locks);
		for(int i = 0; i < NUM_THREADS - 1; i++) {
			threads[i].start();
		}
		threads[NUM_THREADS - 1].run();
		for(int i = 0; i < NUM_THREADS - 1; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return populationGrid;
	}
}
