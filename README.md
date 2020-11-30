## Standard Product S1-GUNW - AOI Ops Report
Generates report for GUNW on_demand, pleiades processing by request_id.

Job is of type iterative. Input facet is an AOI, and there are no user inputs. The job queries for all standard products associated with an AOI, and generates an AOI_Ops_Report product for that AOI. The report is an excel file with the following tabs:
   * Current Product Status: shows all associated intermediate products, date pairs, hashes, missing slcs & acquisitions per expected GUNW. This allows ops to track progress of products through the system & identify gaps.
   * SLCs: shows all SLCs covered by the AOI that have been localized.
   * Missing SLCs: shows all SLCs that have not been localized.
   * Acquisitions: shows all current acquisitions & their associated SLCs and IPF numbers.
   * Acquisition-Lists: shows all acquisition-lists and their associated full_id_hash.
   * IFG-Configs: shows all ifg-cfgs and their associated full_id_hash.
   * IFGs: shows all S1-GUNWs and their associated full_id_hash.

note: this variant currently does not offer the enumeration report.