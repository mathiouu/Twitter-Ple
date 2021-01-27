<script>
import { Pie, mixins } from "vue-chartjs";
export default {
  extends: Pie,
  props: {
    labels: {
      type: Array,
      default: null,
    },
    data: {
      type: Array,
      default: null,
    },
    colors: {
      type: Array,
      default : null
    },
  },
  mixins: [mixins.reactiveProp],
  data: () => ({
    chartdata: {
      labels: [],
      datasets: [
        {
          // backgroundColor: "#879C83",
          backgroundColor: [],
          data: [],
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
    },
  }),

  mounted() {
    this.chartdata.labels = this.labels;
    this.chartdata.datasets[0].data = this.data;
    this.chartdata.datasets[0].backgroundColor = this.colors;
    this.renderChart(this.chartdata, this.options);
  },
};
</script>