from mrjob.job import MRJob

class MRMatrixMultiply(MRJob):

    def mapper(self, _, line):
        # Input format: matrix_name, row, col, value
        # Example: A 0 1 3   (matrix A, row 0, col 1, value 3)
        matrix, i, j, value = line.split()
        i, j, value = int(i), int(j), int(value)

        if matrix == "A":
            # A[i][k] * B[k][j]
            for col in range(0, 100):  # change size accordingly
                yield (i, col), ("A", j, value)

        elif matrix == "B":
            for row in range(0, 100):  # change size accordingly
                yield (row, j), ("B", i, value)

    def reducer(self, key, values):
        A_vals = {}
        B_vals = {}

        for matrix, idx, val in values:
            if matrix == "A":
                A_vals[idx] = val
            else:
                B_vals[idx] = val

        total = 0
        for k in A_vals:
            if k in B_vals:
                total += A_vals[k] * B_vals[k]

        if total != 0:
            yield key, total


if __name__ == "__main__":
    MRMatrixMultiply.run()
