package org.petuum.ps.common;

import org.petuum.ps.row.RowFactory;
import org.petuum.ps.row.RowTypeId;
import org.petuum.ps.row.RowUpdateFactory;
import org.petuum.ps.row.RowUpdateTypeId;
import org.petuum.ps.row.double_.DenseDoubleRowFactory;
import org.petuum.ps.row.double_.DenseDoubleRowUpdateFactory;
import org.petuum.ps.row.double_.SparseDoubleRowFactory;
import org.petuum.ps.row.double_.SparseDoubleRowUpdateFactory;
import org.petuum.ps.row.float_.DenseFloatRowFactory;
import org.petuum.ps.row.float_.DenseFloatRowUpdateFactory;
import org.petuum.ps.row.float_.SparseFloatRowFactory;
import org.petuum.ps.row.float_.SparseFloatRowUpdateFactory;
import org.petuum.ps.row.int_.DenseIntRowFactory;
import org.petuum.ps.row.int_.DenseIntRowUpdateFactory;
import org.petuum.ps.row.int_.SparseIntRowFactory;
import org.petuum.ps.row.int_.SparseIntRowUpdateFactory;
import org.petuum.ps.row.long_.DenseLongRowFactory;
import org.petuum.ps.row.long_.DenseLongRowUpdateFactory;
import org.petuum.ps.row.long_.SparseLongRowFactory;
import org.petuum.ps.row.long_.SparseLongRowUpdateFactory;

/**
 * Created by aqiao on 3/2/15.
 */
public class FactoryRegistry {

    private static final RowFactory denseDoubleRowFactory = new DenseDoubleRowFactory();
    private static final RowUpdateFactory denseDoubleRowUpdateFactory = new DenseDoubleRowUpdateFactory();
    private static final RowFactory sparseDoubleRowFactory = new SparseDoubleRowFactory();
    private static final RowUpdateFactory sparseDoubleRowUpdateFactory = new SparseDoubleRowUpdateFactory();

    private static final RowFactory denseIntRowFactory = new DenseIntRowFactory();
    private static final RowUpdateFactory denseIntRowUpdateFactory = new DenseIntRowUpdateFactory();
    private static final RowFactory sparseIntRowFactory = new SparseIntRowFactory();
    private static final RowUpdateFactory sparseIntRowUpdateFactory = new SparseIntRowUpdateFactory();

    private static final RowFactory denseFloatRowFactory = new DenseFloatRowFactory();
    private static final RowUpdateFactory denseFloatRowUpdateFactory = new DenseFloatRowUpdateFactory();
    private static final RowFactory sparseFloatRowFactory = new SparseFloatRowFactory();
    private static final RowUpdateFactory sparseFloatRowUpdateFactory = new SparseFloatRowUpdateFactory();

    private static final RowFactory denseLongRowFactory = new DenseLongRowFactory();
    private static final RowUpdateFactory denseLongRowUpdateFactory = new DenseLongRowUpdateFactory();
    private static final RowFactory sparseLongRowFactory = new SparseLongRowFactory();
    private static final RowUpdateFactory sparseLongRowUpdateFactory = new SparseLongRowUpdateFactory();

    public static RowFactory getRowFactory(int rowTypeId) {
        switch (rowTypeId) {
            case RowTypeId.DENSE_DOUBLE:
                return denseDoubleRowFactory;
            case RowTypeId.SPARSE_DOUBLE:
                return sparseDoubleRowFactory;
            case RowTypeId.DENSE_INT:
                return denseIntRowFactory;
            case RowTypeId.SPARSE_INT:
                return sparseIntRowFactory;
            case RowTypeId.DENSE_FLOAT:
                return denseFloatRowFactory;
            case RowTypeId.SPARSE_FLOAT:
                return sparseFloatRowFactory;
            case RowTypeId.DENSE_LONG:
                return denseLongRowFactory;
            case RowTypeId.SPARSE_LONG:
                return sparseLongRowFactory;
            default:
                throw new IllegalArgumentException("Unknown row type id=" + rowTypeId);
        }
    }

    public static RowUpdateFactory getRowUpdateFactory(int rowUpdateTypeId) {
        switch (rowUpdateTypeId) {
            case RowUpdateTypeId.DENSE_DOUBLE:
                return denseDoubleRowUpdateFactory;
            case RowUpdateTypeId.SPARSE_DOUBLE:
                return sparseDoubleRowUpdateFactory;
            case RowUpdateTypeId.DENSE_INT:
                return denseIntRowUpdateFactory;
            case RowUpdateTypeId.SPARSE_INT:
                return sparseIntRowUpdateFactory;
            case RowUpdateTypeId.DENSE_FLOAT:
                return denseFloatRowUpdateFactory;
            case RowUpdateTypeId.SPARSE_FLOAT:
                return sparseFloatRowUpdateFactory;
            case RowUpdateTypeId.DENSE_LONG:
                return denseLongRowUpdateFactory;
            case RowUpdateTypeId.SPARSE_LONG:
                return sparseLongRowUpdateFactory;
            default:
                throw new IllegalArgumentException("Unknown row update type id=" + rowUpdateTypeId);
        }
    }
}
