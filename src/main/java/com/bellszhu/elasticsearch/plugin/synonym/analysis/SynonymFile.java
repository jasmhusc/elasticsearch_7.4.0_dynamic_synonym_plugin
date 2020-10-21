/**
 *
 */
package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import org.apache.lucene.analysis.synonym.SynonymMap;

import java.io.Reader;

/**
 * @author bellszhu
 */
public interface SynonymFile {

    /**
     *  重新加载同义词
     */
    SynonymMap reloadSynonymMap();

    /**
     * 重新加载同义词的条件
     */
    boolean isNeedReloadSynonymMap();

    /**
     * 同义词的来源
     */
    Reader getReader();

}