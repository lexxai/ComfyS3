from io import BytesIO

import numpy as np
import torch
from PIL import Image, ImageOps, ImageSequence

from ..client_s3 import get_s3_instance
from ..logger import logger

S3_INSTANCE = get_s3_instance()


class LoadImageS3:
    @classmethod
    def INPUT_TYPES(s):
        input_dir = S3_INSTANCE.input_dir
        try:
            files = S3_INSTANCE.get_files(prefix=input_dir)
        except Exception:
            files = []
        return {
            "required": {
                "image": (sorted(files), {"image_upload": False}),
                "local_store": ("BOOLEAN", {"default": False}),
            },
        }

    CATEGORY = "ComfyS3"
    RETURN_TYPES = ("IMAGE", "MASK")
    FUNCTION = "load_image"
    LOCAL_FOLDER = "input/"

    def load_image(self, image, local_store=False) -> tuple:
        s3_path = image.strip()
        img = None
        if not local_store:
            image_path = S3_INSTANCE.download_file(
                s3_path=s3_path, local_path=f"{self.LOCAL_FOLDER}{image}"
            )
            if not image_path:
                err = "Failed to download object from S3"
                logger.error(err)
                raise Exception(err)

            img = Image.open(image_path)
        else:
            binary_data = S3_INSTANCE.download_object(s3_path=s3_path)
            if binary_data is None:
                err = "Failed to download binary object from S3"
                logger.error(err)
                raise Exception(err)
            with BytesIO(binary_data) as binary_stream:
                img = Image.open(binary_stream)
                img.load()
            del binary_data

        output_images = []
        output_masks = []
        for i in ImageSequence.Iterator(img):
            i = ImageOps.exif_transpose(i)
            if i.mode == "I":
                i = i.point(lambda i: i * (1 / 255))
            image = i.convert("RGB")
            image = np.array(image).astype(np.float32) / 255.0
            image = torch.from_numpy(image)[None,]
            if "A" in i.getbands():
                mask = np.array(i.getchannel("A")).astype(np.float32) / 255.0
                mask = 1.0 - torch.from_numpy(mask)
            else:
                mask = torch.zeros((64, 64), dtype=torch.float32, device="cpu")
            output_images.append(image)
            output_masks.append(mask.unsqueeze(0))

        if len(output_images) > 1:
            output_image = torch.cat(output_images, dim=0)
            output_mask = torch.cat(output_masks, dim=0)
        else:
            output_image = output_images[0]
            output_mask = output_masks[0]

        return output_image, output_mask
