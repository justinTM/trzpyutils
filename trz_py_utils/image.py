import random
import io
from PIL import Image, ImageDraw, ImageFont
import pkg_resources
import logging as log


def show_bounding_boxes(image_bytes, box_sets, colors):
    """
    Draws bounding boxes on image and shows it with default image viewer.

    :param image_bytes: The image to draw, as bytes.
    :param box_sets: A list of lists of bounding boxes to draw on the image.
    :param colors: A list of colors to use to draw the bounding boxes.
    """
    image = Image.open(io.BytesIO(image_bytes))
    draw = ImageDraw.Draw(image)
    for boxes, color in zip(box_sets, colors):
        for box in boxes:
            left = image.width * box["Left"]
            top = image.height * box["Top"]
            right = (image.width * box["Width"]) + left
            bottom = (image.height * box["Height"]) + top
            draw.rectangle([left, top, right, bottom], outline=color, width=3)
    image.show()


def draw_polygons(img: Image.Image,
                  polygons: list[list[dict[str, int]]],
                  color, width=1):
    """
    Draws polygons on an image and shows it with the default image viewer.

    :param image_bytes: The image to draw, as bytes.
    :param polygons: The list of polygons to draw on the image.
    :param color: The color to use to draw the polygons.

    Example:
        >>> # compare this image with saved test image
        >>> from trz_py_utils import image, file
        >>> from PIL import Image, ImageChops
        >>> p1 = "./tests/images/draw_polygons.png"
        >>> p2 = file.tmp_path(".png")
        >>> # create a black image and draw a box inside it
        >>> img = Image.new("RGB", (100, 30), "green")
        >>> img = image.draw_polygons(img, color="red", polygons=[
        ...     [{"X": 0.1, "Y": 0.1},
        ...      {"X": 0.2, "Y": 0.1},
        ...      {"X": 0.2, "Y": 0},
        ...      {"X": 0.1, "Y": 0}],])
        >>> img.save(p2)
        >>> # compare exiting and generated
        >>> ImageChops.difference(Image.open(p1), Image.open(p2)).getbbox()
    """  # noqa
    draw = ImageDraw.Draw(img)
    for polygon in polygons:
        xys = [(img.width * pt["X"], img.height * pt["Y"]) for pt in polygon]
        draw.polygon(xys, outline=color, width=width)
    return img


def pick_color(i: int = None):
    colors = [
        "blue",
        "red",
        "lime",
        "purple",
        "coral",
        "yellow",
        "turquoise",
        "hotpink",
        "limegreen",
        "darkorange",
    ]
    i = i if i is not None else random.randint(0, len(colors)-1)
    return colors[i % len(colors)]


def get_font(size=10, path: str = None, name="menlo"):
    """Cross-platform search for preferred font, or default font.

    Args:
        size (int, optional): font size. Defaults to 10.
        path (str, optional): custom font filepath. Defaults to None.

    Returns:
        _type_: _description_
    """
    # Replace 'your_package' with the actual name of your package
    root_dir = "trz_py_utils"
    font_relative = "fonts/Menlo-Regular.ttf"
    font_relative = "fonts/Futura.ttc"
    ship_path = pkg_resources.resource_filename(root_dir, font_relative)
    for font_path in [path, ship_path, "Supplemental/Futura.ttc"]:
        if not font_path:
            continue
        try:
            log.info(f"trying to open {font_path}")
            return ImageFont.truetype(font_path, size=size)
        except IOError as e:
            log.info(f"failed to get font: {e}")
            pass
    # Use a default font if Arial Unicode MS is not available
    return ImageFont.load_default(size)


def find_font_to_fit_width(text: str,
                           width: int,
                           min_size=20,
                           max_size=200,
                           drawer: ImageDraw.ImageDraw = None):
    drawer = drawer or ImageDraw.Draw(Image.new("RGB", (width, max_size)))
    font_size = (min_size + max_size) // 2
    font = get_font(font_size)  # load from filepath or default

    while max_size - min_size > 1:
        # Check if the text fits within the specified pixel width
        textbbox = drawer.textbbox((0, 0), text, font=font)
        text_width, _ = textbbox[2] - textbbox[0], textbbox[3] - textbbox[1]
        if text_width <= width:
            min_size = font_size
        else:
            max_size = font_size

        # Calculate the next font size as the midpoint
        font_size = (min_size + max_size) // 2

        # Load the font with the new size
        font = get_font(size=font_size)

    return font


def draw_text_by_pixel_width(img: Image.Image,
                             text: str,
                             xy: tuple[int | float, int | float],
                             width: int | float,
                             anchor="la",
                             outline_color="white",
                             outline_width=0,
                             color="black",
                             min_font_as_percent_of_height=0.03,
                             max_font_as_percent_of_height=0.20,
                             **kwargs):
    """Adds text to image with automatic font size to
    fit a certain pixel width. Uses a sort of binary search to find the font.

    Args:
        drawer (ImageDraw.ImageDraw): ImageDraw.Draw(Image.new())
        text (str): string to draw at x,y
        xy (tuple[int, int]): horizontal and vertical coordinates to draw text
        width (int): pixels wide you want the text to be
        anchor (str): # https://pillow.readthedocs.io/en/stable/handbook/text-anchors.html#text-anchors
        outline_color (str, optional): _description_. Defaults to "white".
        outline_width (int, optional): _description_. Defaults to 3.
        color (str, optional): _description_. Defaults to "black".
        min_font_as_percent_of_height (float, optional): _description_. Defaults to 0.02.
        max_font_as_percent_of_height (float, optional): _description_. Defaults to 0.10.
      
    Example:
        >>> # compare newly-generated image to saved test image
        >>> # make sure default minimum font size is being enforced
        >>> from PIL import Image, ImageChops
        >>> from trz_py_utils import image, file
        >>> img1 = image.draw_text_by_pixel_width(
        ...     img=Image.new("RGB", (100, 100), "green"),
        ...     text="Truck",
        ...     xy=(0.5, 0.5),
        ...     width=0.00000001,  # this should not be possible
        ...     color="black",
        ...     anchor="mm"
        ... )
        >>> img2 = image.draw_text_by_pixel_width(
        ...     img=Image.new("RGB", (100, 100), "green"),
        ...     text="Truck",
        ...     xy=(0.5, 0.5),
        ...     width=0.0001,  # this should be the same size as previous
        ...     color="black",
        ...     anchor="mm"
        ... )
        >>> ImageChops.difference(img1, img2).getbbox()
    """  # noqa
    drawer = ImageDraw.Draw(img)
    img_w, img_h = drawer.im.size

    # convert relative to pixels
    if isinstance(width, float):
        width = width*img_w
    if isinstance(xy[0], float):
        xy = (int(xy[0]*img_w), int(xy[1]*img_h))

    min_size = round(img_h * min_font_as_percent_of_height)
    max_size = round(img_h * max_font_as_percent_of_height)

    font = find_font_to_fit_width(text, width, min_size, max_size, drawer)

    # Draw the main text on top
    drawer.text(xy=xy,
                text=text,
                font=font,
                fill=color,
                stroke_width=outline_width,
                stroke_fill=outline_color,
                anchor=anchor,
                **kwargs)

    return img


def bounding_box_to_polygon(bounding_box: dict[str, float]):
    """
    Convert a bounding box dictionary to a polygon.

    Args:
    - bounding_box (dict): Dictionary with keys 'Top', 'Left', 'Height', and 'Width'.

    Returns:
    - list: List of dictionaries representing a polygon with keys 'X' and 'Y'.
    """  # noqa
    top = bounding_box['Top']
    left = bounding_box['Left']
    height = bounding_box['Height']
    width = bounding_box['Width']

    # Calculate other corners of the bounding box
    top_right = (left + width, top)
    bottom_left = (left, top + height)
    bottom_right = (left + width, top + height)

    # Construct the polygon as a list of dictionaries
    polygon = [
        {'X': left, 'Y': top},
        {'X': top_right[0], 'Y': top_right[1]},
        {'X': bottom_right[0], 'Y': bottom_right[1]},
        {'X': bottom_left[0], 'Y': bottom_left[1]},
    ]

    return polygon


def draw_polygon_labels(img: Image.Image,
                        labels: list[str],
                        bboxs: list[dict[str, int | float]],
                        label_width: int | float = 0.30,
                        label_anchor="lt",
                        outline_color="white",
                        outline_px: int = None,
                        label_color: str = None,
                        bbox_color: str = None,
                        bbox_px: int = None,
                        **kwargs):
    """

    see https://docs.aws.amazon.com/rekognition/latest/APIReference/API_BoundingBox.html

    Args:
        img (Image.Image): _description_
        labels (list[str]): ["label1"]
        bboxs (list[dict[str, float]]): [{Height, Width, Top, Left}]
        label_width (int | float, optional): _description_. Defaults to 0.25.
        label_anchor (str, optional): _description_. Defaults to "la".
        label_outline_color (str, optional): _description_. Defaults to "white".
        label_outline_px (int, optional): _description_. Defaults to 0.
        label_color (str, optional): _description_. Defaults to "black".
        bbox_color (str, optional): _description_. Defaults to "black".
        bbox_px (int, optional): _description_. Defaults to 3.
        min_font_as_percent_of_height (float, optional): _description_. Defaults to 0.02.
        max_font_as_percent_of_height (float, optional): _description_. Defaults to 0.50.

    Raises:
        ValueError: _description_
    """  # noqa
    if len(labels) != len(bboxs):
        raise ValueError(f"{len(labels)} labels but {len(bboxs)} bboxs")

    img = img.copy()

    i = 0
    for label, bbox in zip(labels, bboxs):
        # convert pixel coords to percent coords
        if all([isinstance(v, int) for k, v in bbox.items()]):
            bbox = {
                "Top": bbox["Top"] / img.height,
                "Left": bbox["Left"] / img.width,
                "Width": bbox["Width"] / img.width,
                "Height": bbox["Height"] / img.height,
            }
        bbox_w, bbox_h = bbox["Width"], bbox["Height"]

        # don't make text too small if skinny vertical rectangle
        label_w = label_width*bbox_h if bbox_w < bbox_h else label_width*bbox_w
        label_area_px = (label_w*img.width)*(2*label_w*img.width)

        bbox_area_px = bbox_w*bbox_h * img.width*img.height
        bbox_px = bbox_px or max(int(bbox_area_px / 140000), 1)

        if not outline_px:
            outline_px = int(max(min(bbox_px//2, label_area_px//140000), 1))

        # based on line widths, adjust label xy
        x, y = bbox["Left"], bbox["Top"]
        if label_anchor[1] == "t":
            y = bbox["Top"] + (outline_px+bbox_px)/img.height
        elif label_anchor[1] == "b":
            y = bbox["Top"] - (outline_px)/img.height
        if label_anchor[0] == "l":
            x = bbox["Left"] + (outline_px+bbox_px)/img.width

        color = pick_color(i)
        img = draw_polygons(
            img=img,
            polygons=[bounding_box_to_polygon(bbox)],
            color=bbox_color or color,
            width=bbox_px,
            )

        img = draw_text_by_pixel_width(
            text=label,
            width=label_w,
            img=img,
            xy=(x, y),
            anchor=label_anchor,
            color=label_color or color,
            outline_color=outline_color,
            outline_width=outline_px,
            **kwargs
            )

        i += 1

    return img


# polygons = [
#     [{"X": 0.1, "Y": 0.1},
#      {"X": 0.2, "Y": 0.1},
#      {"X": 0.2, "Y": 0},
#      {"X": 0.1, "Y": 0}],
# ]

# img = Image.new("RGB", (3000, 2000), "green")
# color = pick_color()

# img = draw_polygons(img, polygons, color=color)

# # Example usage
# result_image = draw_text_by_pixel_width(
#     image=img,
#     text="Truck",
#     xy=(100, 1000),
#     width=200,
#     color=pick_color(),
#     )

# # Save or display the result image
# result_image.show()
# # result_image.


# # # https://pillow.readthedocs.io/en/stable/handbook/text-anchors.html#text-anchors
# # text_anchor="la"
# # text_outline_color="white"
# # text_outline_width=3
# # text_color="black"
# # text_width=0.2
# # text_font_min=round(image_height/50)

# # bbox_width=2
# # bbox_color
# # bbox_radius






