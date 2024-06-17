from PIL import Image, ImageStat
from pillow_heif import register_heif_opener
from exif import Image as ExifImage
import numpy as np

def analyze_image(image_path):
    try:
        # Extract EXIF data
        exif_data = extract_exif(image_path)
        print('EXIF Data:', exif_data)

        # Check for typical screen resolution
        register_heif_opener()
        image = Image.open(image_path)
        dimensions = image.size
        print('Dimensions:', dimensions)

        # Analyze image visually for common screen elements
        visual_analysis = analyze_visual_elements(image, dimensions)
        print('Visual Analysis:', visual_analysis)

        return {'exif_data': exif_data, 'dimensions': dimensions, 'visual_analysis': visual_analysis}
    except Exception as e:
        print(f'Error analyzing image: {e}')

def extract_exif(image_path):
    try:
        with open(image_path, 'rb') as image_file:
            image = ExifImage(image_file)
            if image.has_exif:
                return image.list_all()
            else:
                return 'No EXIF data found'
    except Exception as e:
        print(f'Error extracting EXIF data: {e}')
        return 'Error extracting EXIF data'

def analyze_visual_elements(image, dimensions):
    analysis_results = []

    # Example: Check for common screen resolutions (1080p, 1440p, etc.)
    common_resolutions = [
        (1920, 1080),
        (2560, 1440),
        (3840, 2160),
        (1381, 835)
    ]

    if dimensions in common_resolutions:
        analysis_results.append('Common screen resolution detected')

    # Convert image to numpy array for pixel analysis
    image_data = np.array(image)

    # Check for specific pixel patterns (e.g., RGB subpixel patterns in screens)
    sub_pixel_pattern_detected = detect_sub_pixel_pattern(image_data)
    if sub_pixel_pattern_detected:
        analysis_results.append('Subpixel pattern detected')

    # Check for glare/reflection patterns
    glare_pattern_detected = detect_glare_pattern(image_data)
    if glare_pattern_detected:
        analysis_results.append('Glare/reflection pattern detected')

    return analysis_results if analysis_results else ['No specific visual patterns detected']

def detect_sub_pixel_pattern(image_data):
    # Implement your pixel pattern detection logic here
    # For example, you can check for repeating RGB subpixel patterns in a specific region of the image
    height, width, _ = image_data.shape
    for y in range(height):
        for x in range(width):
            r, g, b = image_data[y, x]
            
            # Simple heuristic: Check if RGB values form a specific pattern
            if r > g and r > b and g > b:
                return True
    return False

def detect_glare_pattern(image_data):
    # Implement your glare detection logic here
    # For example, you can check for sudden changes in brightness that might indicate glare
    height, width, _ = image_data.shape
    threshold = 200  # Arbitrary threshold for glare detection
    for y in range(height):
        for x in range(width):
            r, g, b = image_data[y, x]
            
            brightness = (r + g + b) / 3
            if brightness > threshold:
                return True
    return False


result = analyze_image('./images/drone.JPG')
print('Analysis Result:', result)