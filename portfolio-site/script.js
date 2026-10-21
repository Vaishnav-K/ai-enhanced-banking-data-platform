const menuToggle = document.getElementById("menuToggle");
const mainNav = document.getElementById("mainNav");

menuToggle.addEventListener("click", () => {
  const isOpen = mainNav.classList.toggle("open");
  menuToggle.setAttribute("aria-expanded", String(isOpen));
  menuToggle.textContent = isOpen ? "×" : "☰";
});

mainNav.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", () => {
    mainNav.classList.remove("open");
    menuToggle.setAttribute("aria-expanded", "false");
    menuToggle.textContent = "☰";
  });
});


/* Interactive pipeline walkthrough: explanatory content only, no fake live metrics. */
const stageContent = {
  ingest: {kicker:"STAGE 01 / INGESTION",title:"Bring source data in safely.",description:"Load synthetic customer and transaction files, including an incremental batch, as the inputs to the pipeline.",focus:"Repeatable input handling",output:"Raw source files ready for processing",tech:"Python · CSV · AWS S3"},
  transform: {kicker:"STAGE 02 / PROCESSING",title:"Transform data with Spark.",description:"Apply batch and incremental processing to the synthetic banking data using the project's PySpark pipeline.",focus:"Consistent, repeatable transformations",output:"Processed customer and transaction datasets",tech:"Python · PySpark · Spark SQL"},
  quality: {kicker:"STAGE 03 / DATA QUALITY",title:"Catch issues before downstream use.",description:"Run data-quality checks and anomaly screening, then retain a machine-readable report for the run.",focus:"Validation and anomaly screening",output:"Quality results and diagnostic evidence",tech:"Python · Data quality checks"},
  reconcile: {kicker:"STAGE 04 / RECONCILIATION",title:"Compare results across processing paths.",description:"Use reconciliation checks to help identify mismatches between pipeline outputs and expected results.",focus:"Cross-engine and output consistency",output:"Reconciliation outcome",tech:"SQL · Spark · Reconciliation checks"},
  operate: {kicker:"STAGE 05 / ORCHESTRATION",title:"Make the run observable and repeatable.",description:"Run the pipeline through Docker-based Airflow orchestration, validate the generated report, and upload raw inputs to S3.",focus:"Orchestration, status validation and recovery",output:"Pipeline run report and S3 ingestion result",tech:"Docker · Apache Airflow · AWS S3"}
};
const stageButtons = Array.from(document.querySelectorAll(".stage-button"));
const stagePanel = document.getElementById("stage-detail");
stageButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const data = stageContent[button.dataset.stage];
    if (!data) return;
    stageButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", String(active));
    });
    stagePanel.setAttribute("aria-labelledby", button.id);
    ["kicker","title","description","focus","output","tech"].forEach((key) => {
      const node = document.getElementById("stage-" + key);
      if (node) node.textContent = data[key];
    });
  });
});

/* Project filters keep the public work scannable for technical recruiters. */
const filterButtons = Array.from(document.querySelectorAll(".filter-button"));
const projectCards = Array.from(document.querySelectorAll(".project-grid .project-card"));
filterButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const filter = button.dataset.filter;
    filterButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-pressed", String(active));
    });
    projectCards.forEach((card) => {
      const categories = (card.dataset.categories || "").split(/\\s+/);
      card.hidden = filter !== "all" && !categories.includes(filter);
    });
  });
});
