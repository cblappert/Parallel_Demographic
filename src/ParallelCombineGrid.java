import java.util.concurrent.RecursiveAction;

/*
 * Christopher Blappert and Micahel Mitasev
 * 
 * This object merges two grids of populations using fork-join parallelism. Both grids
 * will be put into the grid1.
 */

public class ParallelCombineGrid extends RecursiveAction {

	private static final long serialVersionUID = 2590820667897070332L;
	private static final int SEQUENTIAL_CUTOFF = 5000;
	private int maxX;
	private int maxY;
	private int minX;
	private int minY;
	private int[][] grid1;
	private int[][] grid2;
	
	// Pre: parameters are valid
	// Post: creates a new instance of the object
	public ParallelCombineGrid(int[][] leftgrid, int[][] rightgrid, int minX, int maxX, int minY, int maxY) {
		this.grid1 = leftgrid;
		this.grid2 = rightgrid;
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
	}
	
	// Post: merges the two grids by adding the populations, putting the result in grid1
	@Override
	protected void compute() {
		// Sequential cutoff calculated by the nubmer of cells that will be merged
		if ((maxX - minX) * (maxY - minY) <= SEQUENTIAL_CUTOFF) {
			for (int i = minX; i <= maxX; i++) {
				for (int j = minY; j <= maxY; j++) {
					grid1[i][j] += grid2[i][j];
					grid2[i][j] = 0;
				}
			}
		} else {
			int midX = (maxX + minX)/2;
			ParallelCombineGrid left = new ParallelCombineGrid(grid1, grid2, minX, midX, minY, maxY);
			ParallelCombineGrid right = new ParallelCombineGrid(grid1, grid2, midX + 1, maxX, minY, maxY);
			left.fork();
			right.compute();
			left.join();
		}
	}
}
