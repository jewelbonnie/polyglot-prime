import sys
import json
import os
from frictionless import Package, transform, steps

def validate_package(spec_path, file1, file2, file3, file4,file5, file6, file7, output_path):
    results = {
        "errorsSummary": [],
        "report": None
    }

    try:
        # Load the schema definitions from spec.json
        with open(spec_path) as f:
            spec = json.load(f)

        # Map the files directly from the arguments
        file_mappings = {
            "qe_admin_data": file1,
            "screening_observation_data": file2,
            "screening_location_data": file3,
            "screening_encounter_data": file4,
            "screening_consent_data": file5,
            "screening_resources_data": file6,
            "demographic_data": file7, 
            #"question_answer_reference": "data/QUEST_ANSW.csv", 
        } 
        # Check for missing files
        missing_files = {key: path for key, path in file_mappings.items() if not os.path.isfile(path)}
        if missing_files:
            for resource_name, file_path in missing_files.items():
                results["errorsSummary"].append({
                    "rowNumber": None,
                    "fieldNumber": None,
                    "fieldName": resource_name,
                    "message": f"File for resource '{resource_name}' not found: {file_path}",
                    "type": "file-missing-error"
                })

            # Write errors to output.json and skip further processing
            # with open(output_path, 'w') as json_file:
            #     json.dump(results, json_file, indent=4)
            print(json.dumps(results, indent=4))
            # print(f"Validation skipped due to missing files. Results saved to '{output_path}'.")
            return  # Skip Frictionless validation

        # Create the package descriptor dynamically, inserting paths from `file_mappings`
        resources = []
        for resource in spec["resources"]:
            # Ensure the file exists for the given resource name
            path = file_mappings.get(resource["name"])
            if not path:
                raise FileNotFoundError(f"File for resource '{resource['name']}' not found.")

            # Update the resource dictionary with the path
            resource_with_path = {**resource, "path": path}
            resources.append(resource_with_path)

        # Construct the final package descriptor with dynamic paths
        package_descriptor = {
            "name": "csv-validation-using-ig",
            "resources": resources
        }

        # Load the package with Frictionless
        package = Package(package_descriptor)

        # Transform and validate
        common_transform_steps = [
            ("ORGANIZATION_TYPE", "organization_type"),
            ("FACILITY_STATE", "facility_state"),
            ("ENCOUNTER_CLASS_CODE", "encounter_class_code"),
            ("ENCOUNTER_CLASS_CODE_DESCRIPTION", "encounter_class_code_description"),
            ("ENCOUNTER_STATUS_CODE", "encounter_status_code"), 
            ("ENCOUNTER_TYPE_CODE_DESCRIPTION", "encounter_type_code_description"),
            ("SCREENING_STATUS_CODE", "screening_status_code"),
            ("SCREENING_CODE_DESCRIPTION", "screening_code_description"),
            ("QUESTION_CODE_TEXT", "question_code_text"),
            ("UCUM_UNITS", "ucum_units"),
            ("SDOH_DOMAIN", "sdoh_domain"),
            ("ANSWER_CODE", "answer_code"),
            ("ANSWER_CODE_DESCRIPTION", "answer_code_description"),
            ("GENDER", "gender"), 
             ("EXTENSION_SEX_AT_BIRTH_CODE_VALUE", "extension_sex_at_birth_code_value"),
            ("RELATIONSHIP_PERSON_CODE", "relationship_person_code"),
            ("RELATIONSHIP_PERSON_DESCRIPTION", "relationship_person_description"),
            ("STATE", "state"),
            ("EXTENSION_GENDER_IDENTITY_DISPLAY", "extension_gender_identity_display"),            
            ("GENDER_IDENTITY_CODE", "gender_identity_code"),
            ("GENDER_IDENTITY_CODE_DESCRIPTION", "gender_identity_code_description"),
            ("GENDER_IDENTITY_CODE_SYSTEM_NAME", "gender_identity_code_system_name"),
            ("SEXUAL_ORIENTATION_TEXT_STATUS", "sexual_orientation_text_status"),
            ("SEXUAL_ORIENTATION_VALUE_CODE", "sexual_orientation_value_code"),
            ("SEXUAL_ORIENTATION_VALUE_CODE_DESCRIPTION", "sexual_orientation_value_code_description"),
            ("PREFERRED_LANGUAGE_CODE", "preferred_language_code"),
            ("PREFERRED_LANGUAGE_CODE_DESCRIPTION", "preferred_language_code_description"),
            ("PREFERRED_LANGUAGE_CODE_SYSTEM_NAME", "preferred_language_code_system_name"), 
            ("EXTENSION_OMBCATEGORY_ETHNICITY_CODE_DESCRIPTION", "extension_ombcategory_ethnicity_code_description"), 
            ("SCREENING_LANGUAGE", "screening_language"),
            ("CONSENT_SCOPE_CODE", "consent_scope_code"),
            ("CONSENT_STATUS", "consent_status"),
            ("CONSENT_CATEGORY_IDSCL_CODE","consent_category_idscl_code"),
            ("CONSENT_CATEGORY_LOINC_DISPLAY", "consent_category_loinc_display"),   
            ("CONSENT_PROVISION_TYPE", "consent_provision_type"), 
            ("OBSERVATION_CATEGORY_SDOH_CODE",""),
            ("OBSERVATION_CATEGORY_SDOH_CODE","observation_category_sdoh_code"),
            ("OBSERVATION_CATEGORY_SDOH_DISPLAY","observation_category_sdoh_display"),
            ("OBSERVATION_CATEGORY_SOCIAL_HISTORY_CODE","observation_category_social_history_code"),
            ("OBSERVATION_CATEGORY_SURVEY_CODE","observation_category_survey_code"),
            ("QUESTION_CODE_DISPLAY","question_code_display"),
            ("LANGUAGE", "language")
            # ("QUESTION_CODE_REFERENCE", "question_code_reference"), 
            # ("ANSWER_CODE_REFERENCE", "answer_code_reference")
        ]

        for resource in package.resources:
            # Create transform steps only for fields that exist in the current resource
            transform_steps = [
                steps.cell_convert(field_name=field_name, function=lambda value: value.lower())
                for field_name, _ in common_transform_steps
                if any(field.name == field_name for field in resource.schema.fields)
            ]
            resource = transform(resource, steps=transform_steps)

        # Validate the package
        report = package.validate()

        # Add the validation report to results
        results["report"] = report.to_dict()

    except FileNotFoundError as e:
        results["errorsSummary"].append({
            "rowNumber": None,
            "fieldNumber": None,
            "fieldName": None,
            "message": str(e),
            "type": "file-missing-error"
        })

    except Exception as e:
        results["errorsSummary"].append({
            "rowNumber": None,
            "fieldNumber": None,
            "fieldName": None,
            "message": str(e),
            "type": "unexpected-error"
        })

    # Write the results to a JSON file
    # with open(output_path, 'w') as json_file:
    #     json.dump(results, json_file, indent=4)
    print(json.dumps(results, indent=4))


    # Print a success or error message to the console
    # if results["errorsSummary"]:
    #     print(f"Validation completed with errors. Results saved to '{output_path}'.")
    # else:
    #     print(f"Validation completed successfully. Results saved to '{output_path}'.")

if __name__ == "__main__":

    results = {
        "errorsSummary": [],
        "report": None
    }
    # print(len(sys.argv))
    # Check for the correct number of arguments
    if len(sys.argv) != 10: 
        error_message = "Invalid number of arguments. Please provide the following arguments: <spec_path> <file1> <file2> <file3> <file4> <file5> <file6> <file7> <output_path>"
        results["errorsSummary"].append({
        "rowNumber": None,
        "fieldNumber": None,
        "fieldName": None,
        "message": error_message,
        "type": "argument-error"
        })
        # with open("output.json", 'w') as json_file:
        #     json.dump(results, json_file, indent=4)
        print(json.dumps(results, indent=4))
        sys.exit(1)

    # Parse arguments
    spec_path = sys.argv[1]
    file1 = sys.argv[2]
    file2 = sys.argv[3]
    file3 = sys.argv[4]
    file4 = sys.argv[5]
    file5 = sys.argv[6]
    file6 = sys.argv[7]
    file7 = sys.argv[8]
    output_path = sys.argv[9]

    # Check if output path is valid
    # if not output_path.endswith('.json'):
    #     print(f"Warning: Provided output path '{output_path}' is not a valid JSON file. Defaulting to 'output.json'.")
    #     output_path = "output.json"

    # Check if the paths exist
    if not os.path.isfile(spec_path):
        error_message = f"Error: Specification file '{spec_path}' not found."
        results["errorsSummary"].append({
            "rowNumber": None,
            "fieldNumber": None,
            "fieldName": None,
            "message": error_message,
            "type": "file-missing-error"
        })
        # with open(output_path, 'w') as json_file:
        #     json.dump(results, json_file, indent=4) 
        print(json.dumps(results, indent=4))
        sys.exit(1)        

    # Run validation
    validate_package(spec_path, file1, file2, file3, file4,file5, file6, file7, output_path)