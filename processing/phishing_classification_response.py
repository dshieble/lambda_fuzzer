from dataclasses import dataclass, field
import json
import numpy as np

import PIL.Image
import PIL
from typing import Any, Dict, List, Optional, Union
from matplotlib import pyplot as plt
from PIL import Image
import os
from typing import Optional
import numpy as np
import cv2

from utilities.utilities import Jsonable


def vis(img: Image, pred_boxes: np.array):
  '''
  Visualize rcnn predictions
  :param img: PIL.Image
  :param pred_boxes: torch.Tensor of shape Nx4, bounding box coordinates in (x1, y1, x2, y2)
  :param pred_classes: torch.Tensor of shape Nx1 0 for logo, 1 for input, 2 for button, 3 for label(text near input), 4 for block
  :return None
  '''

  check = np.asarray(img)
  if pred_boxes is None or len(pred_boxes) == 0:
    return check
  # pred_boxes = pred_boxes.numpy() if not isinstance(pred_boxes, np.ndarray) else pred_boxes

  # draw rectangle
  print("pred_boxes", pred_boxes)
  for j, box in enumerate(pred_boxes):
    print(box)
    if j == 0:
      cv2.rectangle(check, (int(box[0]), int(box[1])), (int(box[2]), int(box[3])), (255, 255, 0), 2)
    else:
      cv2.rectangle(check, (int(box[0]), int(box[1])), (int(box[2]), int(box[3])), (36, 255, 12), 2)

  return check


@dataclass
class PhishingClassificationResponse(Jsonable):
  attribute_recomputation_id: Optional[str] = None
  pred_target: Optional[str] = None
  siamese_conf: Optional[float] = None
  pred_boxes: Optional[Union[np.array, list]] = None
  matched_coord: Optional[Union[np.array, list]] = None
  screenshot_path: Optional[str] = None
  img: Optional[PIL.Image.Image] = None

  @classmethod
  def from_json_dict(cls, json_dict: dict) -> "PhishingClassificationResponse":
    return cls(**json_dict)

  def to_json_dict(self) -> str:
    return {k: v for k,v in self.__dict__.items() if k not in ('img')}

  def to_json_string(self) -> str:
    return json.dumps(self.to_json_dict())


  def display(self):
    img = Image.open(self.screenshot_path).convert("RGB")
    plotvis = vis(img=img, pred_boxes=self.pred_boxes)
    if self.pred_target is not None:
      # Visualize, add annotations
      cv2.putText(
        plotvis, "Target: {} with confidence {:.4f}".format(self.pred_target, self.siamese_conf),
                  (int(self.matched_coord[0] + 20), int(self.matched_coord[1] + 20)),
                  cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 0), 2)
    print('Phishing (True) or Benign (False) ?', self.pred_target is not None)
    print('What is its targeted brand if it is a phishing ?', self.pred_target)
    print('What is the siamese matching confidence ?', self.siamese_conf)
    print('Where is the predicted logo (in [x_min, y_min, x_max, y_max])?', self.pred_boxes)
    plt.figure(figsize=(20, 20))
    plt.imshow(plotvis[:, :, ::-1])
    plt.imshow(plotvis)

    plt.title("Predicted screenshot with annotations")
    plt.show()




@dataclass
class PhishingClassificationResponseFactory:
  clf: "PhishpediaClassifier"

  def build(
    self,
    processed_url: "ProcessedUrl",
    attribute_recomputation_id: Optional[str] = None
  ) -> "PhishingClassificationResponse":
    if processed_url.url_screenshot_response.screenshot_path is not None:
      phishing_classification_response = self.clf.predict(
        url=processed_url.url_screenshot_response.url, screenshot_path=processed_url.url_screenshot_response.screenshot_path)
    else:
      phishing_classification_response = PhishingClassificationResponse()
    phishing_classification_response.attribute_recomputation_id = attribute_recomputation_id
    return phishing_classification_response