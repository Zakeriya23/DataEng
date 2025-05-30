import requests
from datetime import datetime

# Get current date for filename
today = datetime.now().strftime("%Y-%m-%d")
filename = f"{today}.txt"

vehicle_ids = [
    2901, 2902, 2904, 2905, 2907, 2908, 2910, 2922, 2924, 2926, 2929, 2935, 2937, 3001, 3002, 3004, 3006,
    3008, 3009, 3010, 3017, 3020, 3029, 3036, 3042, 3046, 3052, 3054, 3059, 3105, 3108, 3110, 3115, 3117,
    3118, 3121, 3122, 3125, 3127, 3128, 3132, 3138, 3141, 3146, 3149, 3150, 3153, 3158, 3160, 3163, 3203,
    3204, 3206, 3208, 3213, 3214, 3219, 3220, 3223, 3226, 3227, 3231, 3233, 3234, 3235, 3237, 3240, 3241,
    3242, 3243, 3247, 3249, 3250, 3251, 3252, 3254, 3255, 3261, 3264, 3265, 3315, 3318, 3320, 3324, 3330,
    3404, 3406, 3412, 3417, 3418, 3422, 3503, 3508, 3512, 3521, 3523, 3528, 3534, 3536, 3548, 3549, 3554,
    3557, 3562, 3563, 3565, 3569, 3577, 3603, 3620, 3631, 3633, 3635, 3637, 3639, 3645, 3647, 3649, 3650,
    3702, 3704, 3705, 3712, 3718, 3720, 3723, 3725, 3730, 3731, 3732, 3733, 3737, 3738, 3748, 3749, 3750,
    3754, 3755, 3756, 3902, 3907, 3911, 3912, 3914, 3920, 3922, 3923, 3926, 3928, 3932, 3934, 3935, 3938,
    3939, 3943, 3948, 3951, 3952, 3960, 3963, 3964, 4002, 4006, 4009, 4017, 4019, 4022, 4026, 4031, 4033,
    4038, 4043, 4049, 4053, 4058, 4060, 4062, 4070, 4202, 4206, 4208, 4209, 4213, 4217, 4218, 4223, 4225,
    4226, 4228, 4231, 4236, 4237, 4238, 4302, 4510, 4513, 4521, 4526, 4528, 4530
]

base_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

with open(filename, "w", encoding="utf-8") as f:
    for vehicle_id in vehicle_ids:
        url = f"{base_url}{vehicle_id}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            f.write(f"--- Vehicle ID: {vehicle_id} ---\n")
            f.write(response.text + "\n\n")
        except requests.RequestException as e:
            f.write(f"--- Vehicle ID: {vehicle_id} ---\n")
            f.write(f"Error fetching data: {e}\n\n")