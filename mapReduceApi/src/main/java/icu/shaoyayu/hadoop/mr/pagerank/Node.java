package icu.shaoyayu.hadoop.mr.pagerank;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author 邵涯语
 * @date 2020/4/19 22:18
 * @Version :
 */
public class Node {
    /** 初始值 */
    private Double pageRank = 1d;
    /** 出链 */
    private String[] adjacentNodeNames;
    public static final char fieldSeparator = '\t';

    public Double getPageRank() {
        return pageRank;
    }

    public Node setPageRank(Double pageRank) {
        this.pageRank = pageRank;
        return this;
    }

    public String[] getAdjacentNodeNames() {
        return adjacentNodeNames;
    }

    public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
        this.adjacentNodeNames = adjacentNodeNames;
        return this;
    }

    public boolean containsAdjacentNodes() {
        return adjacentNodeNames != null && adjacentNodeNames.length > 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pageRank);

        if (getAdjacentNodeNames() != null) {
            sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
        }
        return sb.toString();
    }

    // value =1.0 B D
    public static Node fromMR(String value) throws IOException {
        String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeparator);
        if (parts.length < 1) {
            throw new IOException("Expected 1 or more parts but received " + parts.length);
        }
        Node node = new Node().setPageRank(Double.valueOf(parts[0]));
        if (parts.length > 1) {
            node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
        }
        return node;
    }

}