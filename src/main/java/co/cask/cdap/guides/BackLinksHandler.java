/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.guides;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Handles the backlink information submitted through POST.
 */
public class BackLinksHandler extends AbstractHttpServiceHandler {

  static final Pattern URL_DELIMITER = Pattern.compile("\\s+");

  @UseDataSet("backLinks")
  private ObjectStore<String> backLinks;

  @Path("backlink")
  @POST
  public void handleBackLink(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();

    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.NO_CONTENT.code(), "Request content is empty.");
      return;
    }

    if (parseAndStore(Charsets.UTF_8.decode(requestContents).toString().trim())) {
      responder.sendStatus(HttpResponseStatus.OK.code());
    } else {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.code(), "Malformed backlink information");
    }
  }

  /**
   * Parses and stores the backlink information if valid
   *
   * @param bLink the request body
   * @return true if the backlink information is valid else false
   */
  private boolean parseAndStore(String bLink) {
    String[] backlinkURLs = URL_DELIMITER.split(bLink);
    if (backlinkURLs.length == 2) {
      backLinks.write(getIdAsByte(UUID.randomUUID()), bLink);
      return true;
    }
    return false;
  }

  /**
   * Converts a {@link UUID#randomUUID()} to byte[]
   *
   * @param uuid the random {@link UUID}
   * @return {@link UUID} as a byte[]
   */
  private byte[] getIdAsByte(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}
