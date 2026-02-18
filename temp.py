Gemini said
Exploration of Automated API Onboarding to Kong Konnect
Document Metadata

Author: Fadi Tannous

Date: February 18, 2026

Status: 🔴 Blocked (Automation not currently feasible)

Impact: Current PI and Upcoming PI

Executive Summary
This document outlines the findings from the exploration task to automate the onboarding of our self-service container deployments and expose them behind the Kong Konnect gateway.

Following a detailed assessment and discussions with the App Patterns team, it has been determined that it is not currently possible to automate the onboarding of an API to Kong. For at least the current PI and the upcoming PI, users will be required to manually onboard APIs to Kong.

1. Current Manual Onboarding Process
As part of the current workflow for onboarding an API to Kong, users must manually execute a series of prerequisite steps before they can even submit the Kong API Dev Connect template.

These manual steps include:

Kong Onboarding Form: Users must fill out the initial App Patterns | Form.

SwaggerHub Onboarding:

Users must request the specific Sailpoint ACC group.

A manual request must be sent to the team to get assigned to the correct project (in our specific case, the project is named "edp legacy").

Reference: Swagger Hub Access & Sign-In Process.

Manual OpenAPI Spec (OAS) Input:

The OpenAPI Spec must be manually inputted into SwaggerHub.

There are strict formatting requirements, including adding a specific info section, securityschemas section, and other configurations.

Reference: Kong API Gateway for MCP Servers Documentation

2. Roadblocks to Custom Automation (Template Limitations)
An attempt was made to copy the existing Kong API onboarding template into our own workflows to drive automation. However, this is currently blocked due to several missing infrastructural requirements and dependencies on our end:

DevPortal Auth Token: Our environment currently lacks this token. Implementing this requires dedicated coordination with the Dev Connect team to establish the setup.

Kong Konnect Auth: We need to establish authenticated communication directly with Kong. This effort requires joint collaboration with both the Kong team and the Dev Connect team.

Temporary Storage Conflicts (Redis): The existing template relies on Redis for temporary storage. To utilize a similar approach, we must coordinate with the API COE team to secure access and ensure our operations do not create data conflicts.

3. App Patterns Team Alignment
A meeting was held with the App Patterns team to discuss the feasibility of full automation. The summary of their feedback confirms the current technical limitations:

No Current Automation Path: It is strictly not possible to automate API onboarding to Kong at this time.

Migration to Prupath: The App Patterns and Dev Connect teams are currently transitioning to Prupath. Once completed, Prupath is expected to automate all aspects of the pipeline except for OpenAPI Spec (OAS) generation and review.

Mandatory Manual OAS Review: Even with future improvements, it remains a strict requirement to manually review the OpenAPI Spec. There are currently no plans on their roadmap to automate this review phase.

Lack of Exposable APIs for Onboarding: There are currently no plans to expose the "onboard API to Kong" functionality as a consumable API itself, even assuming the prerequisite OAS generation and reviews were fully automated.

4. Outcome and Next Steps
Conclusion: Based on the technical blockers and upstream team roadmaps, fully automated deployments to Kong Konnect cannot be achieved in the short term.

Action Items:

Communicate Manual Expectation: Ensure all relevant stakeholders and deployment teams are aware that API onboarding to Kong will remain a manual process for the current PI and the upcoming PI.

Monitor Prupath Rollout: Keep in touch with the App Patterns and Dev Connect teams regarding the timeline for the Prupath migration to leverage partial automation in future PIs.

Standardize Manual Playbooks: Ensure the documentation linked in Section 1 is up-to-date so development teams have clear guidance on the manual execution steps.
