/**
 * Copyright (c) 2015, Salesforce.com, Inc. All rights reserved.
 * Copyright (c) 2020, Bitshift (bitshifted.co), Inc. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the
 * distribution.
 * 
 * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or
 * promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package co.bitshfted.xapps.zsync;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


import co.bitshfted.xapps.zsync.Zsync.Options;
import co.bitshfted.xapps.zsync.http.ContentRange;
import co.bitshfted.xapps.zsync.internal.util.Stopwatch;

public class ZsyncStatsObserver extends ZsyncObserver {

  public static interface ZsyncStats {

    long getTotalBytesRead();

    long getTotalBytesWritten();

    long getTotalBytesDownloaded();

    long getBytesDownloadedForControlFile();

    long getBytesDownloadedFromRemoteFile();

    Map<Path, Long> getTotalBytesWrittenByInputFile();

    Map<Path, Long> getTotalBytesReadByInputFile();

    long getTotalElapsedMilliseconds();

    long getElapsedMillisecondsDownloading();

    long getElapsedMillisecondsDownloadingControlFile();

    long getElapsedMillisecondsDownloadingRemoteFile();

    Map<List<ContentRange>, Long> getElapsedMillisecondsDownloadingRemoteFileByRequest();

    long getBytesToDownload();

    long getDownloaded();
  }


  // time

  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private final Stopwatch downloadStopwatch = Stopwatch.createUnstarted();
  private long elapsedMillisDownloading = 0;
  private long elapsedMillisDownloadingControlFile = 0;
  private long elapsedMillisDownloadingRemoteFile = 0;
  private final Map<List<ContentRange>, Long> elapsedMillisByRangeRequest = new HashMap<>();
  private List<ContentRange> ranges;

  // data

  private final Map<Path, Long> bytesWrittenByInputFile = new HashMap<>();
  private final Map<Path, Long> bytesReadByInputFile = new HashMap<>();

  private long bytesRead = 0;
  private long bytesWritten = 0;
  private long bytesDownloaded = 0;

  private long totalBytesRead = 0;
  private long totalBytesWritten = 0;
  private long totalBytesDownloaded = 0;

  private long bytesDownloadedForControlFile = 0;
  private long bytesDownloadedFromRemoteTarget = 0;

  private Path inputFile;
  private long bytesReadBefore;
  private long bytesWrittenBefore;

  private long bytesToDownload;
  private long downloaded;

  @Override
  public void zsyncStarted(URI requestedZsyncUri, Options options) {
    this.stopwatch.start();
  }

  @Override
  public void controlFileDownloadingStarted(URI uri, long length) {
    this.bytesDownloaded = 0;
  }

  @Override
  public void controlFileDownloadingInitiated(URI uri) {
    this.downloadStopwatch.start();
  }

  @Override
  public void controlFileDownloadingComplete() {
    this.elapsedMillisDownloadingControlFile += this.downloadStopwatch.stop().elapsed(MILLISECONDS);
    this.downloadStopwatch.reset();
    this.elapsedMillisDownloading += this.elapsedMillisDownloadingControlFile;
    this.bytesDownloadedForControlFile = this.bytesDownloaded;
    this.totalBytesDownloaded += this.bytesDownloaded;
    this.bytesDownloaded = 0;
  }

  @Override
  public void controlFileReadingStarted(Path path, long length) {
    this.bytesRead = 0;
  }

  @Override
  public void controlFileReadingComplete() {
    this.totalBytesRead += this.bytesDownloaded;
    this.totalBytesRead = 0;
  }

  @Override
  public void inputFileReadingStarted(Path inputFile, long length) {
    this.inputFile = inputFile;
    this.bytesReadBefore = this.totalBytesRead;
    this.bytesWrittenBefore = this.totalBytesWritten;
    this.bytesRead = 0;
  }

  @Override
  public void inputFileReadingComplete() {
    this.totalBytesRead += this.bytesRead;
    this.bytesReadByInputFile.put(this.inputFile, this.totalBytesRead - this.bytesReadBefore);
    this.bytesWrittenByInputFile.put(this.inputFile, this.totalBytesWritten - this.bytesWrittenBefore);
    this.inputFile = null;
    this.bytesReadBefore = 0;
    this.bytesWrittenBefore = 0;
    this.bytesRead = 0;
  }

  @Override
  public void outputFileWritingStarted(Path outputFile, long length) {
    this.bytesWritten = 0;
  }

  @Override
  public void outputFileWritingCompleted() {
    this.totalBytesWritten += this.bytesWritten;
    this.bytesWritten = 0;
  }

  @Override
  public void remoteFileDownloadingInitiated(URI uri, List<ContentRange> ranges) {
    this.downloadStopwatch.start();
    this.ranges = ranges;
  }

  @Override
  public void remoteFileDownloadingStarted(URI uri, long length) {
    this.bytesDownloaded = 0;
  }

  @Override
  public void remoteFileDownloadingComplete() {
    final long millis = this.downloadStopwatch.stop().elapsed(MILLISECONDS);
    this.downloadStopwatch.reset();
    this.elapsedMillisDownloadingRemoteFile += millis;
    this.elapsedMillisDownloading += millis;
    this.elapsedMillisByRangeRequest.put(this.ranges, millis);
    this.ranges = null;
    this.bytesDownloadedFromRemoteTarget += this.bytesDownloaded;
    this.totalBytesDownloaded += this.bytesDownloaded;
    this.bytesDownloaded = 0;
  }

  @Override
  public void bytesDownloaded(long bytes) {
    this.bytesDownloaded += bytes;
  }

  @Override
  public void bytesRead(long bytes) {
    this.bytesRead += bytes;
  }

  @Override
  public void bytesWritten(long bytes) {
    this.bytesWritten += bytes;
  }

  @Override
  public void zsyncComplete() {
    this.stopwatch.stop();
  }

  @Override
  public void bytesToDownload(long bytes) {
    this.bytesToDownload = bytes;
  }

  @Override
  public void downloaded(long bytes) {
    this.downloaded += bytes;
  }

  public ZsyncStats build() {
    final Map<Path, Long> bytesWrittenByInputFile = Collections.unmodifiableMap(this.bytesWrittenByInputFile);
    final Map<Path, Long> bytesReadByInputFile = Collections.unmodifiableMap(this.bytesReadByInputFile);
    final long totalElapsedMilliseconds = this.stopwatch.elapsed(TimeUnit.MILLISECONDS);
    final long elapsedMillisecondsDownloading = this.elapsedMillisDownloading;
    final long elapsedMillisecondsDownloadingControlFile = this.elapsedMillisDownloadingControlFile;
    final long elapsedMillisecondsDownloadingRemoteFile = this.elapsedMillisDownloadingRemoteFile;
    final Map<List<ContentRange>, Long> elapsedMillisByRangeRequest = Collections.unmodifiableMap(this.elapsedMillisByRangeRequest);
    final long totalBytesDownloaded = this.totalBytesDownloaded;
    final long bytesDownloadedForControlFile = this.bytesDownloadedForControlFile;
    final long bytesDownloadedFromRemoteTarget = this.bytesDownloadedFromRemoteTarget;
    final long totalBytesRead = this.totalBytesRead;
    final long totalBytesWritten = this.totalBytesWritten;
    final long bytesToDownload = this.bytesToDownload;
    final long downloaded = this.downloaded;

    return new ZsyncStats() {
      @Override
      public long getTotalBytesDownloaded() {
        return totalBytesDownloaded;
      }

      @Override
      public long getBytesDownloadedForControlFile() {
        return bytesDownloadedForControlFile;
      }

      @Override
      public long getBytesDownloadedFromRemoteFile() {
        return bytesDownloadedFromRemoteTarget;
      }

      @Override
      public long getTotalBytesRead() {
        return totalBytesRead;
      }

      @Override
      public long getTotalBytesWritten() {
        return totalBytesWritten;
      }

      @Override
      public Map<Path, Long> getTotalBytesWrittenByInputFile() {
        return bytesWrittenByInputFile;
      }

      @Override
      public Map<Path, Long> getTotalBytesReadByInputFile() {
        return bytesReadByInputFile;
      }

      @Override
      public long getTotalElapsedMilliseconds() {
        return totalElapsedMilliseconds;
      }

      @Override
      public long getElapsedMillisecondsDownloading() {
        return elapsedMillisecondsDownloading;
      }

      @Override
      public long getElapsedMillisecondsDownloadingControlFile() {
        return elapsedMillisecondsDownloadingControlFile;
      }

      @Override
      public long getElapsedMillisecondsDownloadingRemoteFile() {
        return elapsedMillisecondsDownloadingRemoteFile;
      }

      @Override
      public Map<List<ContentRange>, Long> getElapsedMillisecondsDownloadingRemoteFileByRequest() {
        return elapsedMillisByRangeRequest;
      }

      @Override
      public long getBytesToDownload() {
        return bytesToDownload;
      }

      @Override
      public long getDownloaded() {
        return downloaded;
      }
    };
  }
}
